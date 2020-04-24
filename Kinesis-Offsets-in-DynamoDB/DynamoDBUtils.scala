package main.scala

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.spec.{QuerySpec, UpdateItemSpec}
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.model.ScanRequest

import scala.collection.JavaConverters._
import com.amazonaws.services.dynamodbv2.document.AttributeUpdate


object DynamoDBUtils {
  val client = AmazonDynamoDBClientBuilder.standard().build()
  val dynamo = new DynamoDB(client)

  def main(args: Array[String]): Unit = {
    val appName="spark-streaming-demo"

    saveCheckPoint(appName)
    //restoreCheckPoint(appName)
  }

  def saveCheckPoint(appName: String) = {

    //val items = getCheckPoint(appName)
    val scanRequest = new ScanRequest().withTableName(appName).withProjectionExpression("leaseKey,checkpoint,checkpointSubSequenceNumber")
    val result = client.scan(scanRequest)
    val items =  result.getItems.asScala.toSeq

    val checkpointTableName = appName+"-checkpoint"
    val table = dynamo.getTable(checkpointTableName)

    val checkpointKey="checkpoint"
    val checkpointSubSequenceNumberKey="checkpointSubSequenceNumber"
    val hashKey="leaseKey"
    val rangeKey="batch"
    val expiry="expiry"
    val epoch: Long = System.currentTimeMillis / 1000
    val epochExpiry = epoch + 630000

    for (item <- items) {
      val updateItemSpec = new UpdateItemSpec().withPrimaryKey(hashKey, item.get(hashKey).getS(), rangeKey, epoch)
        .withAttributeUpdate( new AttributeUpdate(checkpointKey).put(item.get(checkpointKey).getS()),
          new AttributeUpdate(expiry).put(epochExpiry),
          new AttributeUpdate(checkpointSubSequenceNumberKey).put(BigInt(item.get(checkpointSubSequenceNumberKey).getN())))
        .withReturnValues(ReturnValue.ALL_NEW)

      val outcome = table.updateItem(updateItemSpec)
      System.out.println(outcome.getItem.toJSONPretty)
    }

  }

  def restoreCheckPoint(appName: String) = {

    val checkpointTableName = appName + "-checkpoint"

    // get latest checkpoint from checkpoint table
    val querySpec = new QuerySpec()
      .withScanIndexForward(false)
      .withHashKey("leaseKey", "shardId-000000000001")
      .withAttributesToGet("batch")
      .withMaxResultSize(1)

    val checkpointTable = dynamo.getTable(checkpointTableName)
    val items = checkpointTable.query(querySpec).asScala.toSeq

    if (!items.isEmpty) {

      val latest_batch = items.head.getBigInteger("batch")
      System.out.println(latest_batch)

      // get data for latest checkpoint
      val queryIndexSpec = new QuerySpec()
        .withScanIndexForward(false)
        .withHashKey("batch", latest_batch)

      val gsi = checkpointTable.getIndex("batch-leaseKey-index")
      val items1 = gsi.query(queryIndexSpec).asScala.toSeq

      val key = "leaseKey"
      val checkpointKey = "checkpoint"
      val checkpointSubSequenceNumberKey = "checkpointSubSequenceNumber"
      val table = dynamo.getTable(appName)

      //restore checkpoint to original table
      for (item <- items1) {

        val updateItemSpec = new UpdateItemSpec().withPrimaryKey(key, item.getString(key))
          .withAttributeUpdate(new AttributeUpdate(checkpointKey).put(item.getString(checkpointKey)),
            new AttributeUpdate(checkpointSubSequenceNumberKey).put(item.getNumber(checkpointSubSequenceNumberKey)))
          .withReturnValues(ReturnValue.ALL_NEW)

        val outcome = table.updateItem(updateItemSpec)
        System.out.println(outcome.getItem.toJSONPretty)
      }
    }
  }

}




