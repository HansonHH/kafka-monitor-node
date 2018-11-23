import { ConsumerGroup, Message } from 'kafka-node'
import { logger } from '../utils/logger'
import { ConsumerServiceOptions, ExtendedConsumerGroupOptions, BrokerInfo, TopicPartitionAssignment } from './types'
import { Subject, bindNodeCallback } from 'rxjs'

export class ConsumerService {

    private consumerGroup: ConsumerGroup
    private isConsumerGroupReady$ = new Subject<void>()
    private monitoringTopic: string
    private metrics = new Map<number, { partition: number, host: string, port: string, latency: number }>()
    private startTime = Date.now()
    private brokerInfo: BrokerInfo = {}
    private topicPartitionAssignment: TopicPartitionAssignment = {}

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
        this.consumerGroup.on('error', (error) => this.onError(error))
        this.consumerGroup.on('rebalancing', () => this.onRebalancing())
        this.consumerGroup.on('rebalanced', () => this.onRebalanced())
        this.consumerGroup.on('message', (message) => this.messageProcessing(message))
    }

    private onError(error: Error) {
        logger.error('Consumer group got error: ', error)
    }

    private onRebalancing() {
        logger.info('Consumer group is rebalancing')
    }

    private async onRebalanced() {
        logger.info('Consumer group is ready')
        this.startTime = Date.now()
        this.isConsumerGroupReady$.complete()

        const metadata = await this.loadMetadataForTopics([this.monitoringTopic])
        this.brokerInfo = this.parseBrokerInfo(metadata)
        logger.info('Got broker info', JSON.stringify(this.brokerInfo, null, 2))

        this.topicPartitionAssignment = this.parseTopicPartitionAssignment(metadata)
        logger.info('Got topic partition assignment', JSON.stringify(this.topicPartitionAssignment, null, 2))
    }

    private loadMetadataForTopics(topics?: string[]) {
        logger.info('Loading metadata for topics')
        return bindNodeCallback((this.consumerGroup.client as any).loadMetadataForTopics)
            .call(this.consumerGroup.client, topics).toPromise()
    }

    private parseBrokerInfo(metadata: any): BrokerInfo {
        logger.info('Parsing broker info')
        return metadata[0]
    }

    private parseTopicPartitionAssignment(metadata: any): TopicPartitionAssignment {
        logger.info('Parsing monitoring topic partition assignment')
        return metadata[1].metadata[this.monitoringTopic]
    }

    private messageProcessing(message: Message) {
        const messageValue = JSON.parse(message.value.toString())

        if (messageValue.timestamp < this.startTime) {
            return
        }

        const latency = Date.now() - messageValue.timestamp
        if (message.partition !== undefined && latency >= 0) {
            const leaderBroker = this.topicPartitionAssignment[message.partition].leader
            const brokerInfo = this.brokerInfo[leaderBroker]

            this.metrics.set(brokerInfo.nodeId, {
                partition: message.partition,
                host: brokerInfo.host,
                port: brokerInfo.port,
                latency
            })
            this.printOut()
        }
    }

    printOut() {
        this.metrics.forEach((value, key) => {
            logger.info(`leader: ${key} partition: ${value.partition} host: ${value.host} latency: ${value.latency}ms`)
        })
    }

}