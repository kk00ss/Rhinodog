package rhinodog.Core

import scala.collection.mutable

package object Utils {
    def getConfigParam[T](analyzerConfig: mutable.Map[String,String],
                          name: String,
                          defaultValue: T,
                          func: (String) => T): T = {
        analyzerConfig.get(name) match {
            case Some(x) => try { func(x) }
            catch {
                case e: Exception => println("cannot parse analyzerConfig " +
                    "parameter \""+name+"\" "+e.getMessage)
                    defaultValue
            }
            case _ => defaultValue
        }
    }
}
