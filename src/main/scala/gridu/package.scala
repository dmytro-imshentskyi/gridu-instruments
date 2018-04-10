import java.sql.Date

package object gridu {

  case class Instrument(id: String, date: Date, value: Double)

}
