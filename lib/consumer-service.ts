import { ConsumerGroup, Message } from 'kafka-node'
import { logger } from '../utils/logger'
import { ConsumerServiceOptions, ExtendedConsumerGroupOptions } from './types'
import { Subject } from 'rxjs'

export class ConsumerService {

    private consumerGroup: ConsumerGroup
    private isConsumerGroupReady$ = new Subject<void>()
    private monitoringTopic: string
    private metrics = new Map<number, number>()
    private startTime = Date.now()

    constructor(private options: ConsumerServiceOptions) {
        this.monitoringTopic = this.options.topic || '_monitoring'
        this.consumerGroup = new ConsumerGroup(this.kafkaConsumerGroupOptions(), this.monitoringTopic)
        this.listenEvents()
    }

    stop() {
        this.consumerGroup.close(() => logger.info('Kafka consumer group is closed'))
        this.isConsumerGroupReady$.complete()
    }

    isReady() {
        return this.isConsumerGroupReady$.toPromise()
    }

    private kafkaConsumerGroupOptions() {
        return {
            kafkaHost: this.options.bootstrapServers,
            sslOptions: this.options.sslOptions,
            groupId: 'kafka-monitor-node-group',
            id: 'kafka-monitor-node-member-0',
            sessionTimeout: 15000,
            autoCommit: true,
            autoCommitIntervalMs: 5000,
            fromOffset: 'latest',
            outOfRangeOffset: 'latest',
            commitOffsetsOnFirstJoin: true
        } as ExtendedConsumerGroupOptions
    }

    private listenEvents() {
        this.consumerGroup.on('error', (error) => logger.error('Consumer group got error: ', error))
        this.consumerGroup.on('rebalancing', () => logger.error('Consumer group is rebalancing'))
        this.consumerGroup.on('rebalanced', () => {
            logger.info('Consumer group is ready')
            this.startTime = Date.now()
            this.isConsumerGroupReady$.complete()
        })
        this.consumerGroup.on('message', (message) => this.messageProcessing(message))
    }

    private messageProcessing(message: Message) {
        const messageValue = JSON.parse(message.value.toString())

        if (messageValue.timestamp < this.startTime) {
            return
        }
        const latency = Date.now() - messageValue.timestamp
        if (message.partition !== undefined && latency >= 0) {
            this.metrics.set(message.partition, latency)
            this.printOut()
        }
    }

    printOut() {
        this.metrics.forEach((value, key) => {
            logger.info(`Partition: ${key}, latency: ${value} ms`)
        })
    }

}