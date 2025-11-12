/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.service.storage.s3.sign;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class S3LocationUtilTest {

  @Test
  public void testNormalizeS3Scheme_s3Uri() {
    String uri = "s3://bucket/path/to/file";
    assertThat(S3LocationUtil.normalizeS3Scheme(uri)).isEqualTo(uri);
  }

  @Test
  public void testNormalizeS3Scheme_s3aUri() {
    String uri = "s3a://bucket/path/to/file";
    assertThat(S3LocationUtil.normalizeS3Scheme(uri)).isEqualTo("s3://bucket/path/to/file");
  }

  @Test
  public void testNormalizeS3Scheme_virtualHostedStyle() {
    String uri = "https://bucket.s3.us-west-2.amazonaws.com/path/to/file";
    assertThat(S3LocationUtil.normalizeS3Scheme(uri)).isEqualTo("s3://bucket/path/to/file");
  }

  @Test
  public void testNormalizeS3Scheme_virtualHostedStyleNoRegion() {
    String uri = "https://bucket.s3.amazonaws.com/path/to/file";
    assertThat(S3LocationUtil.normalizeS3Scheme(uri)).isEqualTo("s3://bucket/path/to/file");
  }

  @Test
  public void testNormalizeS3Scheme_pathStyle() {
    String uri = "https://s3.us-west-2.amazonaws.com/bucket/path/to/file";
    assertThat(S3LocationUtil.normalizeS3Scheme(uri)).isEqualTo("s3://bucket/path/to/file");
  }

  @Test
  public void testNormalizeS3Scheme_legacyPathStyle() {
    String uri = "https://s3.amazonaws.com/bucket/path/to/file";
    assertThat(S3LocationUtil.normalizeS3Scheme(uri)).isEqualTo("s3://bucket/path/to/file");
  }

  @Test
  public void testNormalizeS3Scheme_httpUri() {
    String uri = "http://bucket.s3.us-east-1.amazonaws.com/path/to/file";
    assertThat(S3LocationUtil.normalizeS3Scheme(uri)).isEqualTo("s3://bucket/path/to/file");
  }

  @Test
  public void testNormalizeS3Scheme_nullUri() {
    assertThat(S3LocationUtil.normalizeS3Scheme(null)).isNull();
  }

  @Test
  public void testCheckLocation_exactMatch() {
    String warehouse = "s3://bucket/warehouse";
    String requested = "s3://bucket/warehouse/db/table/data.parquet";
    String allowed = "s3://bucket/warehouse/db/table";

    assertThat(S3LocationUtil.checkLocation(warehouse, requested, allowed)).isTrue();
  }

  @Test
  public void testCheckLocation_directPrefix() {
    String warehouse = "s3://bucket/warehouse";
    String requested = "s3://bucket/warehouse/db/table/data/file.parquet";
    String allowed = "s3://bucket/warehouse/db/table/";

    assertThat(S3LocationUtil.checkLocation(warehouse, requested, allowed)).isTrue();
  }

  @Test
  public void testCheckLocation_notAllowed() {
    String warehouse = "s3://bucket/warehouse";
    String requested = "s3://bucket/warehouse/db/other_table/data.parquet";
    String allowed = "s3://bucket/warehouse/db/table";

    assertThat(S3LocationUtil.checkLocation(warehouse, requested, allowed)).isFalse();
  }

  @Test
  public void testCheckLocation_withObjectStorageLayout() {
    // Iceberg 1.7.0+ object storage layout with hash-based paths
    String warehouse = "s3://bucket/warehouse";
    String requested =
        "s3://bucket/warehouse/1000/1000/1110/10001000/db/table/data.parquet";
    String allowed = "s3://bucket/warehouse/db/table/";

    assertThat(S3LocationUtil.checkLocation(warehouse, requested, allowed)).isTrue();
  }

  @Test
  public void testCheckLocation_withObjectStorageLayout_notAllowed() {
    String warehouse = "s3://bucket/warehouse";
    String requested =
        "s3://bucket/warehouse/1000/1000/1110/10001000/db/other_table/data.parquet";
    String allowed = "s3://bucket/warehouse/db/table/";

    assertThat(S3LocationUtil.checkLocation(warehouse, requested, allowed)).isFalse();
  }

  @Test
  public void testCheckLocation_legacyLayout() {
    // Legacy layout where first path element after warehouse is skipped
    String warehouse = "s3://bucket/warehouse";
    String requested = "s3://bucket/warehouse/random-uuid/db/table/data.parquet";
    String allowed = "s3://bucket/warehouse/db/table/";

    assertThat(S3LocationUtil.checkLocation(warehouse, requested, allowed)).isTrue();
  }

  @Test
  public void testCheckLocation_legacyLayout_notAllowed() {
    String warehouse = "s3://bucket/warehouse";
    String requested = "s3://bucket/warehouse/random-uuid/db/other_table/data.parquet";
    String allowed = "s3://bucket/warehouse/db/table/";

    assertThat(S3LocationUtil.checkLocation(warehouse, requested, allowed)).isFalse();
  }

  @Test
  public void testCheckLocation_warehouseWithTrailingSlash() {
    String warehouse = "s3://bucket/warehouse/";
    String requested = "s3://bucket/warehouse/db/table/data.parquet";
    String allowed = "s3://bucket/warehouse/db/table";

    assertThat(S3LocationUtil.checkLocation(warehouse, requested, allowed)).isTrue();
  }

  @Test
  public void testCheckLocation_allowedWithTrailingSlash() {
    String warehouse = "s3://bucket/warehouse";
    String requested = "s3://bucket/warehouse/db/table/data.parquet";
    String allowed = "s3://bucket/warehouse/db/table/";

    assertThat(S3LocationUtil.checkLocation(warehouse, requested, allowed)).isTrue();
  }

  @Test
  public void testCheckLocation_requestedNotInWarehouse() {
    String warehouse = "s3://bucket/warehouse";
    String requested = "s3://other-bucket/data.parquet";
    String allowed = "s3://bucket/warehouse/db/table";

    assertThat(S3LocationUtil.checkLocation(warehouse, requested, allowed)).isFalse();
  }

  @Test
  public void testCheckLocation_allowedNotInWarehouse() {
    String warehouse = "s3://bucket/warehouse";
    String requested = "s3://bucket/warehouse/db/table/data.parquet";
    String allowed = "s3://other-bucket/data";

    assertThat(S3LocationUtil.checkLocation(warehouse, requested, allowed)).isFalse();
  }

  @Test
  public void testCheckLocation_emptyWarehouse() {
    String warehouse = "";
    String requested = "s3://bucket/data.parquet";
    String allowed = "s3://bucket/";

    assertThat(S3LocationUtil.checkLocation(warehouse, requested, allowed)).isFalse();
  }

  @Test
  public void testCheckLocation_complexPath() {
    String warehouse = "s3://my-bucket/iceberg/warehouse";
    String requested =
        "s3://my-bucket/iceberg/warehouse/mydb/mytable/data/00000-0-abc123.parquet";
    String allowed = "s3://my-bucket/iceberg/warehouse/mydb/mytable/";

    assertThat(S3LocationUtil.checkLocation(warehouse, requested, allowed)).isTrue();
  }
}

