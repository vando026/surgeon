package conviva.surgeon

import conviva.surgeon.Sanitize._
import conviva.surgeon.Donor._ 
import conviva.surgeon.Paths._ 
import conviva.surgeon.PbSS._ 

object heart {

  case class Testy(in: String) {
    def pp = println(in)
  }

}
