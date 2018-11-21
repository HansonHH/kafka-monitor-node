import 'mocha'
import { expect } from 'chai'
import { fillUpRecord } from '../utils/util'

describe('utils test', () => {
    describe('fillUpRecord function', () => {
        it('recordSizeByte > original record size', () => {
            const recordSizeByte = 100
            const record = { timestamp: Date.now(), dummy: '' }
            const originalRecordSizeByte = Buffer.from(JSON.stringify(record)).length

            const newRecord = fillUpRecord(record, recordSizeByte)
            const newRecordSizeByte = Buffer.from(JSON.stringify(newRecord)).length

            expect(Buffer.from(newRecord.dummy).length).to.equal(recordSizeByte - originalRecordSizeByte)
            expect(newRecordSizeByte).to.equal(recordSizeByte)
        })

        it('recordSizeByte < original record size', () => {
            const recordSizeByte = 10
            const record = { timestamp: Date.now(), dummy: '' }
            const originalRecordSizeByte = Buffer.from(JSON.stringify(record)).length

            const newRecord = fillUpRecord(record, recordSizeByte)
            const newRecordSizeByte = Buffer.from(JSON.stringify(newRecord)).length


            expect(Buffer.from(newRecord.dummy).length).not.to.equal(recordSizeByte - originalRecordSizeByte)
            expect(newRecordSizeByte).not.to.equal(recordSizeByte)
            expect(newRecordSizeByte).to.equal(originalRecordSizeByte)
        })
    })
})
