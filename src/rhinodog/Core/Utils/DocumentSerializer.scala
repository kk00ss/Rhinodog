// Copyright 2016 Konstantin Voznesenskiy

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package rhinodog.Core.Utils

import java.nio.ByteBuffer

import rhinodog.Core.Definitions._

class DocumentSerializer
(measureSerializer: MeasureSerializerBase) {
    def serialize (doc: AnalyzedDocument): DocumentSerialized = {
        val termsLength = 4 + doc.terms.length*(4+measureSerializer.numberOfBytesRequired)
        val binStr = doc.text.getBytes
        val strLength = 4 + binStr.length
        val estimatedSize = termsLength + strLength
        val output = ByteBuffer.allocate(estimatedSize)
        output.putInt(doc.terms.length)
        for(term <- doc.terms) {
            output.putInt(term.termID)
            measureSerializer.serialize(term.measure, output)
        }
        output.putInt(binStr.length)
        output.put(binStr)
        return output.array()
    }
    def deserialize (documentSerialized: DocumentSerialized): AnalyzedDocument = {
        val input = ByteBuffer.wrap(documentSerialized)
        val numOfTerms = input.getInt()
        val terms = new Array[DocTerm] (numOfTerms)
        for(i <- 0 until numOfTerms) {
            val termID = input.getInt()
            val measure = measureSerializer.deserialize(input)
            terms(i) = DocTerm(termID, measure)
        }
        val strLen = input.getInt()
        val strArr = new Array[Byte] (strLen)
        input.get(strArr, 0, strLen)
        val text = new String(strArr)
        return AnalyzedDocument(text, terms)
    }

}
