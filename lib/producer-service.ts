import { Producer, KafkaClient, KafkaClientOptions, ProducerOptions } from 'kafka-node'
import { Subject, interval } from 'rxjs'
import { map, takeUntil } from 'rxjs/operators'
import { logger } from '../utils/logger'

export class ProducerService {
    private client: KafkaClient
    private producer: Producer
    private isClientReady$ = new Subject<void>()
    private isProducerReady$ = new Subject<void>()
    private runOnTerm$ = new Subject<void>()
    private monitoringTopic: string

    constructor(private clientOptions: KafkaClientOptions, private producerOptions: ProducerOptions) {
        this.clientOptions = clientOptions
        this.client = new KafkaClient(this.clientOptions)
        this.producer = new Producer(this.client, {
            ...this.producerOptions,
            partitionerType: 2,
        })
        this.monitoringTopic = '_monitoring'
        this.init()
    }

    async start() {
        logger.info('ProducerServer is starting')
        await Promise.all([this.isClientReady$.toPromise(), this.isProducerReady$.toPromise()])

        const topicMetadata = (await this.loadMetadataForTopics()) as any
        const brokers = topicMetadata[0]
        const clusterScale = Object.keys(brokers).length

        try {
            await this.ifMonitoringTopicExists().catch(async () => {
                logger.info('Monitoring topic does not exist')
                await this.createMonitoringTopic(clusterScale, clusterScale)
            })

            this.startInterval()
        } catch (e) {
            logger.error('Failed to start producer service')
        }
    }

    stop() {
        this.runOnTerm$.complete()
        this.producer.close()
        this.client.close()
        this.isProducerReady$.complete()
        this.isClientReady$.complete()
    }

    private init() {
        this.client.on('ready', () => {
            logger.info('Kafka client is ready')
            this.isClientReady$.complete()
        })
        this.client.on('brokersChanged', () => logger.info('Kafka client brokersChanged'))
        this.client.on('error', (error) => logger.error('Kafka client got error: ', error))
        this.producer.on('error', (error) => logger.error('Producer got error: ', error))
        this.producer.on('ready', () => {
            logger.info('Producer is ready')
            this.isProducerReady$.complete()
        })
    }

    private async ifMonitoringTopicExists() {
        logger.info('Checking if monitoring topic exists')
        return new Promise((resolve, reject) => {
            this.client.topicExists([this.monitoringTopic], (error) => {
                if (error) {
                    reject(error)
                }
                resolve()
            })
        })
    }

    private async createMonitoringTopic(partitions: number, replicationFactor: number) {
        logger.info('Creating monitoring topic')
        return new Promise((resolve, reject) => {
            ;(this.client as any).createTopics(
                [
                    {
                        topic: this.monitoringTopic,
                        partitions,
                        replicationFactor,
                    },
                ] as any,
                (error: any, result: any) => {
                    if (error) {
                        reject(error)
                    }
                    resolve(result)
                }
            )
        })
    }

    private loadMetadataForTopics() {
        return new Promise((resolve, reject) => {
            ;(this.client as any).loadMetadataForTopics([], (error: any, result: any) => {
                if (error) {
                    reject(error)
                }
                resolve(result)
            })
        })
    }

    private startInterval() {
        logger.info('staring interval')
        return interval(1000)
            .pipe(
                map(() => {
                    logger.info('New tick')
                    this.sendToKafka().catch((error) => logger.error('Failed to send to Kafka: ', error))
                }),
                takeUntil(this.runOnTerm$)
            )
            .toPromise()
    }

    private sendToKafka() {
        return new Promise((resolve, reject) => {
            this.producer.send(
                [
                    {
                        topic: this.monitoringTopic,
                        messages: JSON.stringify({ timestamp: Date.now() }),
                    },
                ],
                (error, result) => {
                    if (error) {
                        reject(error)
                    }
                    resolve(result)
                }
            )
        })
    }
}
