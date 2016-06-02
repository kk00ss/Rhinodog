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
