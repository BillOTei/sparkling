package models

import models.Department.DepartmentId

import scala.collection.immutable.ListMap

case class Department(id: DepartmentId, name: String)

object Department {
  type DepartmentId = Int

  lazy val departments = ListMap(
    0 -> Department(0, "Dept. 0"),
    1 -> Department(1, "Dept. 1"),
    2 -> Department(2, "Dept. 2"),
    3 -> Department(3, "Dept. 3"),
    4 -> Department(4, "Dept. 4"),
    5 -> Department(5, "Dept. 5"),
    6 -> Department(6, "Dept. 6"),
    7 -> Department(7, "Dept. 7"),
    8 -> Department(8, "Dept. 8"),
    9 -> Department(9, "Dept. 9"),
    10 -> Department(10, "Dept. 10"),
    11 -> Department(11, "Dept. 11"),
    12 -> Department(12, "Dept. 12"),
    13 -> Department(13, "Dept. 13"),
    14 -> Department(14, "Dept. 14"),
    15 -> Department(15, "Dept. 15")
  )
}