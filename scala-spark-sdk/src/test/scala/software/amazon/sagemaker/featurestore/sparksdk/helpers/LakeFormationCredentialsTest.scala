package software.amazon.sagemaker.featurestore.sparksdk.helpers

import org.scalatestplus.testng.TestNGSuite
import org.testng.Assert.{assertFalse, assertTrue}
import org.testng.annotations.Test

import java.time.Instant

class LakeFormationCredentialsTest extends TestNGSuite {

  @Test
  def testIsExpiringSoonReturnsTrueWhenWithinBuffer(): Unit = {
    val creds = LakeFormationCredentials(
      accessKeyId = "ak",
      secretAccessKey = "sk",
      sessionToken = "st",
      expiration = Instant.now().plusSeconds(60),
      region = "us-west-2",
      accountId = "123456789012",
      partition = "aws",
      database = "db",
      table = "tbl"
    )
    assertTrue(creds.isExpiringSoon(300))
  }

  @Test
  def testIsExpiringSoonReturnsFalseWhenFarAway(): Unit = {
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
    assertFalse(creds.isExpiringSoon(300))
  }
}
