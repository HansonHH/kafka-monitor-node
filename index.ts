import { ProducerService } from './lib/producer-service'

async function main() {
    const producerService = new ProducerService(
        {
            bootstrapServers: '192.168.0.102:32777',
            topic: 'test'
        }
    )
    await producerService.start()
}

main()
