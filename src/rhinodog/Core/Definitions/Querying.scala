package rhinodog.Core.Definitions

import Configuration.MainComponents
import rhinodog.Core.Iterators._

import scala.collection.Seq

object Querying {

    object TermType extends Enumeration {
        type ENUM = this.Value
        val MUST, SHOULD, MUST_NOT = this.Value
    }

    case class QueryTerm(_ID: Int,
                         _type: TermType.ENUM)

}
