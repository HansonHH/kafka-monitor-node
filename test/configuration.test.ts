import 'mocha'
import { expect } from 'chai'
import * as mockery from 'mockery'

mockery.enable({
    warnOnReplace: false,
    warnOnUnregistered: false,
})

class KafkaClientMock {
    constructor() { }
    on() { }
    close() { }
}
class ProducerMock {
    constructor() { }
    on() { }
    close() { }
}
class ConsumerGroupMock {
    constructor() { }
    on() { }
    close() { }
}
mockery.registerMock('kafka-node', {
    KafkaClient: KafkaClientMock,
    Producer: ProducerMock,
    ConsumerGroup: ConsumerGroupMock,
})

import { ProducerService } from '../lib/producer-service'
import { ConsumerService } from '../lib/consumer-service'

describe('Producer service', () => {
    describe('Configure all options', () => {
        let producerService: any

        before(() => {
            producerService = new ProducerService({
                bootstrapServers: '127.0.0.1:1234',
                topic: 'test_topic',
                id: 'test',
                recordDelayMs: 1000,
                recordSizeByte: 1000,
                sslOptions: {
                    ca: 'ca',
                    cert: 'cert',
                    key: 'key',
                    rejectUnauthorized: true,
                    requestCert: true,
                },
            }) as any
        })

        it('should get expected kafka client configuration', () => {
            expect(producerService.kafkaClientOptions()).to.deep.include({
                kafkaHost: '127.0.0.1:1234',
                sslOptions: {
                    ca: 'ca',
                    cert: 'cert',
                    key: 'key',
                    rejectUnauthorized: true,
                    requestCert: true,
                },
                clientId: 'test',
                requestTimeout: 10000,
            })
        })

        it('should get expected kafka producer configuration', () => {
            expect(producerService.kafkaProducerOptions()).to.deep.include({
                requireAcks: 1,
                ackTimeoutMs: 5000,
                partitionerType: 2,
            })
        })

        it('should get expected monitoring topic configuration', () => {
            expect(producerService.monitoringTopic).to.equal('test_topic')
        })

        it('should get expected record delay ms configuration', () => {
            expect(producerService.recordDelayMs).to.equal(1000)
        })

        it('should get expected record size byte configuration', () => {
            expect(producerService.recordSizeByte).to.equal(1000)
        })

        after(() => producerService.stop())
    })

    describe('Use as many default options as possible', () => {
        let producerService: any

        before(() => {
            producerService = new ProducerService({
                bootstrapServers: '127.0.0.1:1234',
            }) as any
        })

        it('should get expected kafka client configuration', () => {
            expect(producerService.kafkaClientOptions()).to.deep.include({
                kafkaHost: '127.0.0.1:1234',
                requestTimeout: 10000,
            })
        })

        it('should get expected kafka producer configuration', () => {
            expect(producerService.kafkaProducerOptions()).to.deep.include({
                requireAcks: 1,
                ackTimeoutMs: 5000,
                partitionerType: 2,
            })
        })

        it('should get expected monitoring topic configuration', () => {
            expect(producerService.monitoringTopic).to.equal('_monitoring')
        })

        it('should get expected record delay ms configuration', () => {
            expect(producerService.recordDelayMs).to.equal(100)
        })

        it('should get expected record size byte configuration', () => {
            expect(producerService.recordSizeByte).to.equal(100)
        })

        after(() => producerService.stop())
    })
})

describe('Consumer service', () => {
    describe('Configure all options', () => {
        let consumerService: any

        before(() => {
            consumerService = new ConsumerService({
                bootstrapServers: '127.0.0.1:1234',
                topic: 'test_topic',
                id: 'test',
                sslOptions: {
                    ca: 'ca',
                    cert: 'cert',
                    key: 'key',
                    rejectUnauthorized: true,
                    requestCert: true,
                },
            }) as any
        })

        it('should get expected kafka consumer group configuration', () => {
            expect(consumerService.kafkaConsumerGroupOptions()).to.deep.include({
                kafkaHost: '127.0.0.1:1234',
                sslOptions: {
                    ca: 'ca',
                    cert: 'cert',
                    key: 'key',
                    rejectUnauthorized: true,
                    requestCert: true,
                },
                groupId: 'kafka-monitor-node-group',
                id: 'kafka-monitor-node-member-0',
                sessionTimeout: 15000,
                autoCommit: true,
                autoCommitIntervalMs: 5000,
                fromOffset: 'latest',
                outOfRangeOffset: 'latest',
                commitOffsetsOnFirstJoin: true,
            })
        })

        it('should get expected monitoring topic configuration', () => {
            expect(consumerService.monitoringTopic).to.equal('test_topic')
        })

        after(() => consumerService.stop())
    })

    describe('Use as many default options as possible', () => {
        let consumerService: any

        before(() => {
            consumerService = new ConsumerService({
                bootstrapServers: '127.0.0.1:1234',
            }) as any
        })

        it('should get expected kafka consumer group configuration', () => {
            expect(consumerService.kafkaConsumerGroupOptions()).to.deep.include({
                kafkaHost: '127.0.0.1:1234',
                groupId: 'kafka-monitor-node-group',
                id: 'kafka-monitor-node-member-0',
                sessionTimeout: 15000,
                autoCommit: true,
                autoCommitIntervalMs: 5000,
                fromOffset: 'latest',
                outOfRangeOffset: 'latest',
                commitOffsetsOnFirstJoin: true,
            })
        })

        it('should get expected monitoring topic configuration', () => {
            expect(consumerService.monitoringTopic).to.equal('_monitoring')
        })

        after(() => consumerService.stop())
    })
})
