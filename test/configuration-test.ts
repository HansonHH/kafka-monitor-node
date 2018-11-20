import 'mocha'
import { expect } from 'chai'
import * as mockery from 'mockery'

mockery.enable({
    warnOnReplace: false,
    warnOnUnregistered: false,
})

class KafkaClientMock {
    constructor() {}
    on() {}
    close() {}
}
class ProducerMock {
    constructor() {}
    on() {}
    close() {}
}
mockery.registerMock('kafka-node', {
    KafkaClient: KafkaClientMock,
    Producer: ProducerMock,
})

import { ProducerService } from '../lib/producer-service'

describe('Producer service', () => {
    describe('Configure all options', () => {
        let producerService: any

        before(() => {
            producerService = new ProducerService({
                bootstrapServers: '127.0.0.1:1234',
                topic: 'test_topic',
                id: 'test',
                intervalMs: 1000,
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

        it('should get expected interval configuration', () => {
            expect(producerService.intervalMs).to.equal(1000)
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

        it('should get expected interval configuration', () => {
            expect(producerService.intervalMs).to.equal(100)
        })

        after(() => producerService.stop())
    })
})
