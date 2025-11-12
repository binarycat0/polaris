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

import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Utility class for S3 location handling and validation. */
public class S3LocationUtil {

  /**
   * Pattern for the new object-storage path layout introduced in Apache Iceberg 1.7.0, checking
   * for the four path elements that contain only {@code 0} or {@code 1}.
   *
   * <p>Example: {@code
   * s3://bucket1/warehouse/1000/1000/1110/10001000/newdb/table_949afb2c-ed93-4702-b390-f1d4a9c59957/my-data-file.txt}
   */
  private static final Pattern NEW_OBJECT_STORAGE_LAYOUT =
      Pattern.compile("[01]{4}/[01]{4}/[01]{4}/[01]{8}/(.*)");

  private S3LocationUtil() {
    // Utility class
  }

  /**
   * Normalizes an S3 URI to use the s3:// scheme.
   *
   * <p>Converts http://, https://, and s3a:// schemes to s3://.
   *
   * @param uri the URI to normalize
   * @return the normalized S3 URI string
   */
  public static String normalizeS3Scheme(String uri) {
    if (uri == null) {
      return null;
    }

    // Convert http(s)://bucket.s3.region.amazonaws.com/path to s3://bucket/path
    if (uri.startsWith("http://") || uri.startsWith("https://")) {
      return convertHttpToS3(uri);
    }

    // Convert s3a:// to s3://
    if (uri.startsWith("s3a://")) {
      return "s3://" + uri.substring(6);
    }

    return uri;
  }

  /**
   * Converts HTTP(S) S3 URLs to s3:// scheme.
   *
   * <p>Handles both virtual-hosted-style and path-style S3 URLs.
   *
   * @param httpUri the HTTP(S) URI
   * @return the s3:// URI
   */
  private static String convertHttpToS3(String httpUri) {
    try {
      URI uri = URI.create(httpUri);
      String host = uri.getHost();
      String path = uri.getPath();

      if (host == null) {
        return httpUri;
      }

      // Virtual-hosted-style: bucket.s3.region.amazonaws.com or bucket.s3.amazonaws.com
      if (host.endsWith(".s3.amazonaws.com") || (host.contains(".s3.") && host.endsWith(".amazonaws.com"))) {
        String bucket = host.substring(0, host.indexOf(".s3."));
        return "s3://" + bucket + (path != null ? path : "");
      }

      // Path-style: s3.region.amazonaws.com/bucket/key
      if (host.startsWith("s3.") && host.endsWith(".amazonaws.com") && path != null && !path.isEmpty()) {
        // Remove leading slash and extract bucket from path
        String pathWithoutSlash = path.startsWith("/") ? path.substring(1) : path;
        return "s3://" + pathWithoutSlash;
      }

      // s3.amazonaws.com/bucket/key (legacy path-style)
      if (host.equals("s3.amazonaws.com") && path != null && !path.isEmpty()) {
        String pathWithoutSlash = path.startsWith("/") ? path.substring(1) : path;
        return "s3://" + pathWithoutSlash;
      }

      return httpUri;
    } catch (Exception e) {
      return httpUri;
    }
  }

  /**
   * Checks if a requested location is allowed based on an allowed location prefix.
   *
   * <p>This method handles both standard paths and Iceberg 1.7.0+ object storage layout with
   * hash-based prefixes.
   *
   * @param warehouseLocation the warehouse base location
   * @param requestedLocation the location being requested for signing
   * @param allowedLocation the allowed location prefix
   * @return true if the requested location is allowed, false otherwise
   */
  public static boolean checkLocation(
      String warehouseLocation, String requestedLocation, String allowedLocation) {
    // Check warehouse is not empty first
    int warehouseLocationLength = warehouseLocation.length();
    if (warehouseLocationLength == 0) {
      return false;
    }

    // Simple prefix check
    if (requestedLocation.startsWith(allowedLocation)) {
      return true;
    }

    // For files that were written with 'write.object-storage.enabled' enabled, repeat the check
    // but ignore the first S3 path element after the warehouse location

    String normalizedWarehouse = warehouseLocation;
    if (!normalizedWarehouse.endsWith("/")) {
      normalizedWarehouse += "/";
      warehouseLocationLength++;
    }

    String normalizedAllowed = allowedLocation;
    if (!normalizedAllowed.endsWith("/")) {
      normalizedAllowed += "/";
    }

    if (!requestedLocation.startsWith(normalizedWarehouse)
        || !normalizedAllowed.startsWith(normalizedWarehouse)) {
      return false;
    }

    String requestedPath = requestedLocation.substring(warehouseLocationLength);

    // Check for Iceberg 1.7.0+ object storage layout (hash-based paths)
    Matcher newObjectStorageLayoutMatcher = NEW_OBJECT_STORAGE_LAYOUT.matcher(requestedPath);
    if (newObjectStorageLayoutMatcher.find()) {
      requestedPath = newObjectStorageLayoutMatcher.group(1);
    } else {
      // Legacy layout: skip the first path element
      int requestedSlash = requestedPath.indexOf('/');
      if (requestedSlash == -1) {
        return false;
      }
      requestedPath = requestedPath.substring(requestedSlash + 1);
    }

    String locationPath = normalizedAllowed.substring(warehouseLocationLength);
    return requestedPath.startsWith(locationPath);
  }
}

