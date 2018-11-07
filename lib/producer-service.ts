import { Producer, KafkaClient } from 'kafka-node'
import { Subject, interval } from 'rxjs'
import { map, takeUntil } from 'rxjs/operators'
import { logger } from '../utils/logger'
import { ProducerServiceOptions } from './types'

export class ProducerService {
    private client: KafkaClient
    private producer: Producer
    private isClientReady$ = new Subject<void>()
    private isProducerReady$ = new Subject<void>()
    private destroy$ = new Subject<void>()
    private monitoringTopic: string
    private intervalMs: number

    constructor(private options: ProducerServiceOptions) {
        this.client = new KafkaClient(this.kafkaClientOptions())
        this.producer = new Producer(this.client, this.kafkaProducerOptions())
        this.monitoringTopic = this.options.topic || '_monitoring'
        this.intervalMs = this.options.intervalMs || 100
        this.init()
    }

    async start() {
        logger.info('ProducerServer is starting')
        await Promise.all([this.isClientReady$.toPromise(), this.isProducerReady$.toPromise()])

        try {
            await this.ifMonitoringTopicExists().catch(async () => {
                logger.info('Monitoring topic does not exist')

                const clusterScale = await this.getClusterScale()
                logger.info('Kafka cluster scale: ', clusterScale)

                await this.createMonitoringTopic(clusterScale, clusterScale)
                logger.info('Created monitoring topic:', this.monitoringTopic)
            })

            logger.info('Monitoring topic exists:', this.monitoringTopic)
            this.startInterval()

        } catch (e) {
            logger.error('Failed to start producer service: ', e)
        }
    }

    stop() {
        this.destroy$.next()
        this.producer.close()
        this.client.close()
        this.isProducerReady$.complete()
        this.isClientReady$.complete()
        this.destroy$.complete()
    }

    private kafkaClientOptions() {
        return {
            kafkaHost: this.options.bootstrapServers,
            sslOptions: this.options.sslOptions,
            clientId: this.options.id,
            requestTimeout: 10 * 1000
        }
    }

    private kafkaProducerOptions() {
        return {
            requireAcks: 1,
            ackTimeoutMs: 5000,
            partitionerType: 2
        }
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

    private async getClusterScale() {
        const topicMetadata = (await this.loadMetadataForTopics()) as any
        const brokers = topicMetadata[0]
        return Object.keys(brokers).length
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
            ; (this.client as any).createTopics(
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
            ; (this.client as any).loadMetadataForTopics([], (error: any, result: any) => {
                if (error) {
                    reject(error)
                }
                resolve(result)
            })
        })
    }

    private startInterval() {
        logger.info('staring interval')
        return interval(this.intervalMs)
            .pipe(
                map(() => {
                    logger.info('New tick')
                    this.sendToKafka().catch((error) => logger.error('Failed to send to Kafka: ', error))
                }),
                takeUntil(this.destroy$)
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
