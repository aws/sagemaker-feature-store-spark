package software.amazon.sagemaker.featurestore.sparksdk.helpers

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.stubbing.Stubber
import org.scalatestplus.testng.TestNGSuite
import org.testng.Assert.{assertEquals, assertFalse, assertTrue}
import org.testng.annotations.{BeforeMethod, Test}
import software.amazon.awssdk.services.lakeformation.LakeFormationClient
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsRequest
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsResponse

import java.time.Instant

class LakeFormationHelperTest extends TestNGSuite {

  // Use Java Mockito API directly to avoid Scala 2.12 reflection issues with
  // AWS SDK v2 builder inner classes (CyclicReference / no symbol could be loaded).
  private val mockLfClient = Mockito.mock(classOf[LakeFormationClient])

  // Disambiguate Mockito.doReturn overloads for Scala 2.12
  private def stubReturn(value: Any): Stubber = Mockito.doReturn(value, Seq.empty[Object]: _*)

  @BeforeMethod
  def setup(): Unit = {
    Mockito.reset(mockLfClient)
    ClientFactory.skipInitialization = true
    ClientFactory.lakeFormationClient = mockLfClient
  }

  @Test
  def testVendCredentialsSuccess(): Unit = {
    val expiration = Instant.now().plusSeconds(3600)

    stubReturn(
      GetTemporaryGlueTableCredentialsResponse
        .builder()
        .accessKeyId("ak")
        .secretAccessKey("sk")
        .sessionToken("st")
        .expiration(expiration)
        .build()
    )
      .when(mockLfClient)
      .getTemporaryGlueTableCredentials(any(classOf[GetTemporaryGlueTableCredentialsRequest]))

    val result = LakeFormationHelper.vendCredentials("us-west-2", "123456789012", "aws", "db", "tbl")
    assertTrue(result.isDefined)
    assertEquals(result.get.region, "us-west-2")
    assertEquals(result.get.accountId, "123456789012")
    assertEquals(result.get.database, "db")
    assertEquals(result.get.table, "tbl")
  }

  @Test
  def testVendCredentialsFailure(): Unit = {
    Mockito
      .doThrow(new RuntimeException("LF error"))
      .when(mockLfClient)
      .getTemporaryGlueTableCredentials(any(classOf[GetTemporaryGlueTableCredentialsRequest]))

    val result = LakeFormationHelper.vendCredentials("us-west-2", "123456789012", "aws", "db", "tbl")
    assertFalse(result.isDefined)
  }

  @Test
  def testRefreshIfNeededWhenExpiringSoon(): Unit = {
    val expiration = Instant.now().plusSeconds(3600)

    stubReturn(
      GetTemporaryGlueTableCredentialsResponse
        .builder()
        .accessKeyId("new-ak")
        .secretAccessKey("new-sk")
        .sessionToken("new-st")
        .expiration(expiration)
        .build()
    )
      .when(mockLfClient)
      .getTemporaryGlueTableCredentials(any(classOf[GetTemporaryGlueTableCredentialsRequest]))

    val expiringSoonCreds = LakeFormationCredentials(
      accessKeyId = "old-ak",
      secretAccessKey = "old-sk",
      sessionToken = "old-st",
      expiration = Instant.now().plusSeconds(60),
      region = "us-west-2",
      accountId = "123456789012",
      partition = "aws",
      database = "db",
      table = "tbl"
    )

    val result = LakeFormationHelper.refreshIfNeeded(expiringSoonCreds)
    assertTrue(result.isDefined)
    assertEquals(result.get.accessKeyId, "new-ak")
  }

  @Test
  def testRefreshIfNeededWhenNotExpiringSoon(): Unit = {
    val creds = LakeFormationCredentials(
      accessKeyId = "ak",
      secretAccessKey = "sk",
      sessionToken = "st",
      expiration = Instant.now().plusSeconds(3600),
      region = "us-west-2",
      accountId = "123456789012",
      partition = "aws",
      database = "db",
      table = "tbl"
    )

    val result = LakeFormationHelper.refreshIfNeeded(creds)
    assertTrue(result.isDefined)
    assertEquals(result.get.accessKeyId, "ak")
  }

  @Test
  def testRefreshIfNeededChinaRegion(): Unit = {
    val expiration = Instant.now().plusSeconds(3600)

    stubReturn(
      GetTemporaryGlueTableCredentialsResponse
        .builder()
        .accessKeyId("cn-ak")
        .secretAccessKey("cn-sk")
        .sessionToken("cn-st")
        .expiration(expiration)
        .build()
    )
      .when(mockLfClient)
      .getTemporaryGlueTableCredentials(any(classOf[GetTemporaryGlueTableCredentialsRequest]))

    val expiringSoonCreds = LakeFormationCredentials(
      accessKeyId = "old-ak",
      secretAccessKey = "old-sk",
      sessionToken = "old-st",
      expiration = Instant.now().plusSeconds(60),
      region = "cn-north-1",
      accountId = "123456789012",
      partition = "aws-cn",
      database = "db",
      table = "tbl"
    )

    val result = LakeFormationHelper.refreshIfNeeded(expiringSoonCreds)
    assertTrue(result.isDefined)
    assertEquals(result.get.accessKeyId, "cn-ak")
  }

  @Test
  def testRefreshIfNeededGovCloudRegion(): Unit = {
    val expiration = Instant.now().plusSeconds(3600)

    stubReturn(
      GetTemporaryGlueTableCredentialsResponse
        .builder()
        .accessKeyId("gov-ak")
        .secretAccessKey("gov-sk")
        .sessionToken("gov-st")
        .expiration(expiration)
        .build()
    )
      .when(mockLfClient)
      .getTemporaryGlueTableCredentials(any(classOf[GetTemporaryGlueTableCredentialsRequest]))

    val expiringSoonCreds = LakeFormationCredentials(
      accessKeyId = "old-ak",
      secretAccessKey = "old-sk",
      sessionToken = "old-st",
      expiration = Instant.now().plusSeconds(60),
      region = "us-gov-west-1",
      accountId = "123456789012",
      partition = "aws-us-gov",
      database = "db",
      table = "tbl"
    )

    val result = LakeFormationHelper.refreshIfNeeded(expiringSoonCreds)
    assertTrue(result.isDefined)
    assertEquals(result.get.accessKeyId, "gov-ak")
  }

  @Test
  def testBuildGlueTableArn(): Unit = {
    val arn = LakeFormationHelper.buildGlueTableArn("aws", "us-west-2", "123456789012", "db", "tbl")
    assertEquals(arn, "arn:aws:glue:us-west-2:123456789012:table/db/tbl")
  }

  @Test
  def testBuildGlueTableArnChinaPartition(): Unit = {
    val arn = LakeFormationHelper.buildGlueTableArn("aws-cn", "cn-north-1", "123456789012", "db", "tbl")
    assertEquals(arn, "arn:aws-cn:glue:cn-north-1:123456789012:table/db/tbl")
  }

  @Test
  def testBuildGlueTableArnGovPartition(): Unit = {
    val arn = LakeFormationHelper.buildGlueTableArn("aws-us-gov", "us-gov-west-1", "123456789012", "db", "tbl")
    assertEquals(arn, "arn:aws-us-gov:glue:us-gov-west-1:123456789012:table/db/tbl")
  }
}
