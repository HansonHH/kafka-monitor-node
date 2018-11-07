import { ProducerService } from './lib/producer-service'

async function main() {
    const producerService = new ProducerService(
        {
            kafkaHost: '192.168.0.103:32774',
        },
        {}
    )
    await producerService.start()
}

main()
