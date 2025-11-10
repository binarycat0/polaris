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

import java.util.Collection;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.resolver.ResolutionManifestFactory;
import org.apache.polaris.core.storage.InMemoryStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.StorageUtil;
import org.apache.polaris.service.catalog.common.CatalogHandler;
import org.apache.polaris.service.catalog.common.CatalogUtils;
import org.apache.polaris.service.catalog.io.FileIOUtil;
import org.apache.polaris.service.context.catalog.CallContextCatalogFactory;
import org.apache.polaris.service.s3.sign.model.PolarisS3SignRequest;
import org.apache.polaris.service.s3.sign.model.PolarisS3SignResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3RemoteSigningCatalogHandler extends CatalogHandler implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3RemoteSigningCatalogHandler.class);

  private final CallContextCatalogFactory catalogFactory;
  private final S3RequestSigner s3RequestSigner;

  private CatalogEntity catalogEntity;
  private Catalog baseCatalog;

  public S3RemoteSigningCatalogHandler(
      PolarisDiagnostics diagnostics,
      CallContext callContext,
      ResolutionManifestFactory resolutionManifestFactory,
      CallContextCatalogFactory catalogFactory,
      PolarisPrincipal polarisPrincipal,
      String catalogName,
      PolarisAuthorizer authorizer,
      S3RequestSigner s3RequestSigner) {
    super(
        diagnostics,
        callContext,
        resolutionManifestFactory,
        polarisPrincipal,
        catalogName,
        authorizer,
        // external catalogs are not supported for S3 remote signing
        null,
        null);
    this.catalogFactory = catalogFactory;
    this.s3RequestSigner = s3RequestSigner;
  }

  @Override
  protected void initializeCatalog() {
    catalogEntity =
        CatalogEntity.of(resolutionManifest.getResolvedReferenceCatalogEntity().getRawLeafEntity());
    if (catalogEntity.isExternal()) {
      throw new ForbiddenException("Cannot use S3 remote signing with federated catalogs.");
    }
    baseCatalog = catalogFactory.createCallContextCatalog(resolutionManifest);
  }

  public PolarisS3SignResponse signS3Request(
      PolarisS3SignRequest s3SignRequest, TableIdentifier tableIdentifier) {

    LOGGER.debug("Requesting s3 signing for {}: {}", tableIdentifier, s3SignRequest);

    PolarisAuthorizableOperation authzOp =
        s3SignRequest.write()
            ? PolarisAuthorizableOperation.SIGN_S3_WRITE_REQUEST
            : PolarisAuthorizableOperation.SIGN_S3_READ_REQUEST;

    authorizeRemoteSigningOrThrow(EnumSet.of(authzOp), tableIdentifier);

    // Must be done after the authorization check, as the auth check creates the catalog entity
    throwIfRemoteSigningNotEnabled(callContext.getRealmConfig(), catalogEntity);

    var result =
        InMemoryStorageIntegration.validateAllowedLocations(
            callContext.getRealmConfig(),
            getAllowedLocations(tableIdentifier),
            getStorageActions(s3SignRequest),
            getTargetLocations(s3SignRequest));

    if (result.values().stream().anyMatch(r -> r.values().stream().anyMatch(v -> !v.isSuccess()))) {
      throw new ForbiddenException("Requested S3 location is not allowed.");
    }

    PolarisS3SignResponse s3SignResponse = s3RequestSigner.signRequest(s3SignRequest);
    LOGGER.debug("S3 signing response: {}", s3SignResponse);

    return s3SignResponse;
  }

  // TODO M2 computing allowed locations is expensive. We should cache it.
  private Collection<String> getAllowedLocations(TableIdentifier tableIdentifier) {

    if (baseCatalog.tableExists(tableIdentifier)) {

      // If the table exists, get allowed locations from the table metadata
      Table table = baseCatalog.loadTable(tableIdentifier);
      if (table instanceof BaseTable baseTable) {
        return StorageUtil.getLocationsUsedByTable(baseTable.operations().current());
      }

      throw new ForbiddenException("No storage configuration found for table.");

    } else {

      // If the table or view doesn't exist, the engine might be writing the manifests before the
      // table creation is committed. In this case, we still need to check allowed locations from
      // the parent entities.

      PolarisResolvedPathWrapper resolvedPath =
          CatalogUtils.findResolvedStorageEntity(resolutionManifest, tableIdentifier);

      Optional<PolarisEntity> storageInfo = FileIOUtil.findStorageInfoFromHierarchy(resolvedPath);

      var configurationInfo =
          storageInfo
              .map(PolarisEntity::getInternalPropertiesAsMap)
              .map(info -> info.get(PolarisEntityConstants.getStorageConfigInfoPropertyName()))
              .map(PolarisStorageConfigurationInfo::deserialize);

      if (configurationInfo.isEmpty()) {
        throw new ForbiddenException("No storage configuration found for table.");
      }

      return configurationInfo.get().getAllowedLocations();
    }
  }

  private Set<PolarisStorageActions> getStorageActions(PolarisS3SignRequest s3SignRequest) {
    // TODO M2: better mapping of request URIs to storage actions.
    // Disambiguate LIST vs READ or WRITE vs DELETE is not possible based on the HTTP method alone.
    // Examples:
    // - ListObjects is conceptually a LIST operation, and GetObject is conceptually a READ. But
    // both requests use the GET method.
    // - DeleteObject uses the DELETE method, but the DeleteObjects operation uses the POST method.
    return s3SignRequest.write()
        ? Set.of(PolarisStorageActions.WRITE)
        : Set.of(PolarisStorageActions.READ);
  }

  private Set<String> getTargetLocations(PolarisS3SignRequest s3SignRequest) {
    // TODO M2: map http URI to s3 URI
    return Set.of();
  }

  public static void throwIfRemoteSigningNotEnabled(
      RealmConfig realmConfig, CatalogEntity catalogEntity) {
    if (catalogEntity.isExternal()) {
      throw new ForbiddenException("Remote signing is not enabled for external catalogs.");
    }
    boolean remoteSigningEnabled =
        realmConfig.getConfig(FeatureConfiguration.REMOTE_SIGNING_ENABLED, catalogEntity);
    if (!remoteSigningEnabled) {
      throw new ForbiddenException(
          "Remote signing is not enabled for this catalog. To enable this feature, set the Polaris configuration %s "
              + "or the catalog configuration %s.",
          FeatureConfiguration.REMOTE_SIGNING_ENABLED.key(),
          FeatureConfiguration.REMOTE_SIGNING_ENABLED.catalogConfig());
    }
  }

  @Override
  public void close() {}
}
