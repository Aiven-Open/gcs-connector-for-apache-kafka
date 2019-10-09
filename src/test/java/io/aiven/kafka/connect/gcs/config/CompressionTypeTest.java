package io.aiven.kafka.connect.gcs.config;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CompressionTypeTest {


  // Test written by Diffblue Cover.
  @Test
  public void forNameInputNotNullOutputGzip() {

    // Act
    final CompressionType actual = CompressionType.forName("gZIp");

    // Assert result
    assertEquals(CompressionType.GZIP, actual);
  }

  // Test written by Diffblue Cover.
  @Test
  public void forNameInputNotNullOutputNone() {

    // Act
    final CompressionType actual = CompressionType.forName("nOnE");

    // Assert result
    assertEquals(CompressionType.NONE, actual);
  }
}
