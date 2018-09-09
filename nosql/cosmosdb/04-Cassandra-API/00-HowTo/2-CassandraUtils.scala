// Databricks notebook source
// MAGIC %md
// MAGIC # ConnectionFactory with retry policy
// MAGIC These are classes needed for connecting to Cassandra with retry policy
// MAGIC <br>
// MAGIC We need to run these whenever we want to connect to CosmosDB Cassandra API<br>
// MAGIC **Note:**  These classes will be hosted on maven at which point you dont need this notebook.

// COMMAND ----------

package com.microsoft.azure.cosmosdb.cassandra

import com.datastax.driver.core.exceptions._
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision
import com.datastax.driver.core.{ConsistencyLevel, Statement}
import com.datastax.spark.connector.cql.MultipleRetryPolicy


/**
  * This retry policy extends the MultipleRetryPolicy, and additionally performs retries with back-offs for overloaded exceptions. For more details regarding this, please refer to the "Retry Policy" section of README.md
  */
class CosmosDbMultipleRetryPolicy(maxRetryCount: Int)
  extends MultipleRetryPolicy(maxRetryCount){

  /**
    * The retry policy performs growing/fixed back-offs for overloaded exceptions based on the max retries:
    * 1. If Max retries == -1, i.e., retry infinitely, then we follow a fixed back-off scheme of 5 seconds on each retry.
    * 2. If Max retries != -1, and is any positive number n, then we follow a growing back-off scheme of (i*1) seconds where 'i' is the i'th retry.
    * If you'd like to modify the back-off intervals, please update GrowingBackOffTimeMs and FixedBackOffTimeMs accordingly.
    */
  val GrowingBackOffTimeMs: Int = 1000
  val FixedBackOffTimeMs: Int = 5000

  // scalastyle:off null
  private def retryManyTimesWithBackOffOrThrow(nbRetry: Int): RetryDecision = maxRetryCount match {
    case -1 =>
      Thread.sleep(FixedBackOffTimeMs)
      RetryDecision.retry(null)
    case maxRetries =>
      if (nbRetry < maxRetries) {
        Thread.sleep(GrowingBackOffTimeMs * nbRetry)
        RetryDecision.retry(null)
      } else {
        RetryDecision.rethrow()
      }
  }

  override def init(cluster: com.datastax.driver.core.Cluster): Unit = {}
  override def close(): Unit = {}

  override def onRequestError(
                               stmt: Statement,
                               cl: ConsistencyLevel,
                               ex: DriverException,
                               nbRetry: Int): RetryDecision = {
    ex match {
      case _: OverloadedException => retryManyTimesWithBackOffOrThrow(nbRetry)
      case _ => RetryDecision.rethrow()
    }
  }
}

// COMMAND ----------

package com.microsoft.azure.cosmosdb.cassandra

import java.nio.file.{Files, Path, Paths}
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

import com.datastax.driver.core._
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy
import com.datastax.spark.connector.cql.CassandraConnectorConf.CassandraSSLConf
import com.datastax.spark.connector.cql._
import org.apache.commons.io.IOUtils

object CosmosDbConnectionFactory extends CassandraConnectionFactory {

  /** Returns the Cluster.Builder object used to setup Cluster instance. */
  def clusterBuilder(conf: CassandraConnectorConf): Cluster.Builder = {
    val options = new SocketOptions()
      .setConnectTimeoutMillis(conf.connectTimeoutMillis)
      .setReadTimeoutMillis(conf.readTimeoutMillis)

    val builder = Cluster.builder()
      .addContactPoints(conf.hosts.toSeq: _*)
      .withPort(conf.port)
      /**
        * Make use of the custom RetryPolicy for Cosmos DB. This Is needed for retrying scenarios specific to Cosmos DB.
        * Please refer to the "Retry Policy" section of the README.md for more information regarding this.
        */
      .withRetryPolicy(
        new CosmosDbMultipleRetryPolicy(conf.queryRetryCount))
      .withReconnectionPolicy(
        new ExponentialReconnectionPolicy(conf.minReconnectionDelayMillis, conf.maxReconnectionDelayMillis))
      .withLoadBalancingPolicy(
        new LocalNodeFirstLoadBalancingPolicy(conf.hosts, conf.localDC))
      .withAuthProvider(conf.authConf.authProvider)
      .withSocketOptions(options)
      .withCompression(conf.compression)
      .withQueryOptions(
        new QueryOptions()
          .setRefreshNodeIntervalMillis(0)
          .setRefreshNodeListIntervalMillis(0)
          .setRefreshSchemaIntervalMillis(0))

    if (conf.cassandraSSLConf.enabled) {
      maybeCreateSSLOptions(conf.cassandraSSLConf) match {
        case Some(sslOptions) ⇒ builder.withSSL(sslOptions)
        case None ⇒ builder.withSSL()
      }
    } else {
      builder
    }
  }

  private def getKeyStore(
                           ksType: String,
                           ksPassword: Option[String],
                           ksPath: Option[Path]): Option[KeyStore] = {

    ksPath match {
      case Some(path) =>
        val ksIn = Files.newInputStream(path)
        try {
          val keyStore = KeyStore.getInstance(ksType)
          keyStore.load(ksIn, ksPassword.map(_.toCharArray).orNull)
          Some(keyStore)
        } finally {
          IOUtils.closeQuietly(ksIn)
        }
      case None => None
    }
  }

  private def maybeCreateSSLOptions(conf: CassandraSSLConf): Option[SSLOptions] = {
    lazy val trustStore =
      getKeyStore(conf.trustStoreType, conf.trustStorePassword, conf.trustStorePath.map(Paths.get(_)))
    lazy val keyStore =
      getKeyStore(conf.keyStoreType, conf.keyStorePassword, conf.keyStorePath.map(Paths.get(_)))

    if (conf.enabled) {
      val trustManagerFactory = for (ts <- trustStore) yield {
        val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
        tmf.init(ts)
        tmf
      }

      val keyManagerFactory = if (conf.clientAuthEnabled) {
        for (ks <- keyStore) yield {
          val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
          kmf.init(ks, conf.keyStorePassword.map(_.toCharArray).orNull)
          kmf
        }
      } else {
        None
      }

      val context = SSLContext.getInstance(conf.protocol)
      context.init(
        keyManagerFactory.map(_.getKeyManagers).orNull,
        trustManagerFactory.map(_.getTrustManagers).orNull,
        new SecureRandom)

      Some(
        JdkSSLOptions.builder()
          .withSSLContext(context)
          .withCipherSuites(conf.enabledAlgorithms.toArray)
          .build())
    } else {
      None
    }
  }

  /** Creates and configures the Cassandra connection */
  override def createCluster(conf: CassandraConnectorConf): Cluster = {
    clusterBuilder(conf).build()
  }

}