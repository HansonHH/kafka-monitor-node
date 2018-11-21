import { Producer, KafkaClient } from 'kafka-node'
import { Subject, interval, bindCallback, bindNodeCallback, empty } from 'rxjs'
import { map, takeUntil } from 'rxjs/operators'
import { logger } from '../utils/logger'
import { ProducerServiceOptions, Record } from './types'

export class ProducerService {
    private client: KafkaClient
    private producer: Producer
    private isClientReady$ = new Subject<void>()
    private isProducerReady$ = new Subject<void>()
    private destroy$ = new Subject<void>()
    private monitoringTopic: string
    private recordDelayMs: number
    private recordSizeByte: number

    constructor(private options: ProducerServiceOptions) {
        this.client = new KafkaClient(this.kafkaClientOptions())
        this.producer = new Producer(this.client, this.kafkaProducerOptions())
        this.monitoringTopic = this.options.topic || '_monitoring'
        this.recordDelayMs = this.options.recordDelayMs || 100
        this.recordSizeByte = this.options.recordSizeByte || 100
        this.listenEvents()
    }

    async start() {
        logger.info('ProducerService is starting')
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

            const monitoringTopicMetadata = await this.loadMetadataForTopics([this.monitoringTopic])
            logger.info('Monitoring topic metadata', JSON.stringify(monitoringTopicMetadata, null, 2))

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

    private listenEvents() {
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
        const topicMetadata = await this.loadMetadataForTopics([])
        const brokers = topicMetadata[0]
        return Object.keys(brokers).length
    }

    private async ifMonitoringTopicExists() {
        logger.info(`Checking if monitoring topic ${this.monitoringTopic} exists`)
        await bindCallback(this.client.topicExists).call(this.client, ([this.monitoringTopic]))
            .pipe(
                map(
                    (error) => error ? Promise.reject(error) : empty(),
                )).toPromise()
    }

    private async createMonitoringTopic(partitions: number, replicationFactor: number) {
        logger.info('Creating monitoring topic')
        return bindNodeCallback((this.client as any).createTopics).call(this.client, [
            {
                topic: this.monitoringTopic,
                partitions,
                replicationFactor,
            },
        ] as any).toPromise()
    }

    private loadMetadataForTopics(topics?: string[]) {
        logger.info('loading metadata for topics')
        return bindNodeCallback((this.client as any).loadMetadataForTopics).call(this.client, topics).toPromise()
    }

    private sendToKafka() {
        return bindNodeCallback(this.producer.send).call(this.producer, [
            {
                topic: this.monitoringTopic,
                messages: this.createRecord(this.recordSizeByte),
            },
        ]).toPromise()
    }

    private createRecord(recordSizeByte: number) {
        const originalRecord = { timestamp: Date.now(), dummy: '' }
        return this.fillUpRecord(originalRecord, recordSizeByte)
    }

    private fillUpRecord(record: Record, recordSizeByte: number) {
        const recordString = JSON.stringify(record)
        const originalRecordByteLength = Buffer.byteLength(recordString)
        if (recordSizeByte > originalRecordByteLength) {
            const seed = '0'
            record.dummy = seed.repeat(recordSizeByte - originalRecordByteLength)
        }
        return JSON.stringify(record)
    }

    private startInterval() {
        logger.info('Starting ProducerService interval')
        return interval(this.recordDelayMs)
            .pipe(
                map(() => {
                    logger.info(`New tick (every ${this.recordDelayMs} ms)`)
                    this.sendToKafka().catch((error: any) => logger.error('Failed to send to Kafka: ', error))
                }),
                takeUntil(this.destroy$)
            )
            .toPromise()
    }
}
