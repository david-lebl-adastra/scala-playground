package lebldavi.quill

import io.getquill.{NamingStrategy, PostgresEscape, SnakeCase}

trait Database extends NamingStrategy with PostgresEscape:
  override def table(s: String): String   = default(s.stripSuffix("Entity"))
  override def column(s: String): String  = default(s)
  override def default(s: String): String = SnakeCase.default(s)

object Database extends Database
