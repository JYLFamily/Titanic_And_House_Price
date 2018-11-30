package main.kaggle.classification

case class Sample(
  PassengerId: Integer,
  Survived: Integer,
  Pclass: String,
  Name: String,
  Sex: String,
  Age: Double,
  SibSp: Double,
  Parch: Double,
  Ticket: String,
  Fare: Double,
  Cabin: String,
  Embarked: String
)
