package com.alstom.Launcher.Config

object ConfigConstants {

   val FieldSeparator = "#"
   val ReferenceDataFieldSeparator = ";"
   val DataFormat = "yy-MM-dd HH:mm a"
   val HadoopSecurityAuthentication = "simple"
   val HadoopSecurityAuthorization = "false"
   val DfsPermissions = "false"
   val SparkSqlShufflePartitions =  4
   val SparkExecutorMemory = "2g"
   val StreamingMicroBatchSeconds = 10
   val ReferenceDataRefreshInterval = 100 // Seconds


  // Column Names
   val TotalAmount: String = "totalAmount"
   val ClientId = "clientId"
   val ProductId = "productId"
   val TotalAmountOfItemsSold = "totalAmountOfItemsSold"
   val TotalNumberOfItemsSold = "totalNumberOfItemsSold"
   val Stock = "stock"

   val NumberOfItems: String = "numberOfItems"
   val Date :String = "date"
   val Time :String = "time"

  // Columns for partitioning
   val Year = "year"
   val Month = "month"
   val Day = "day"
   val Hour = "hour"


   val TransactionLocalTime :String = "transactionLocalTime"

}
