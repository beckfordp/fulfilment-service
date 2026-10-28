package fulfilmentservice

sealed trait FulfilmentError

case object FulfilmentNotFound extends FulfilmentError

case object InvalidPagination extends FulfilmentError
