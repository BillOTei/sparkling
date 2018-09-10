package models

import models.Department.DepartmentId

case class User(departmentId: DepartmentId, name: String, id: Option[Long] = None)