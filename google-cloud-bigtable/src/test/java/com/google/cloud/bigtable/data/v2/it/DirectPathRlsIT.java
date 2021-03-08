/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.data.v2.it;

import static com.google.api.gax.rpc.StatusCode.Code.UNAVAILABLE;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.TruthJUnit.assume;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.api.core.ApiFunction;
import com.google.api.core.ApiFuture;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.test_helpers.env.AbstractTestEnv;
import com.google.cloud.bigtable.test_helpers.env.TestEnvRule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientCall.Listener;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Grpc;
import io.grpc.InternalConfigSelector;
import io.grpc.LoadBalancer.PickSubchannelArgs;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.NameResolver;
import io.grpc.NameResolver.Args;
import io.grpc.NameResolverRegistry;
import io.grpc.Status;
import io.grpc.alts.ComputeEngineChannelBuilder;
import io.grpc.lookup.v1.RouteLookupRequest;
import io.grpc.lookup.v1.RouteLookupResponse;
import io.grpc.lookup.v1.RouteLookupServiceGrpc;
import io.grpc.lookup.v1.RouteLookupServiceGrpc.RouteLookupServiceBlockingStub;
import io.grpc.testing.GrpcCleanupRule;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test DirectPath Dynamic Routing behaviors.
 *
 * <p>WARNING: this test can only be run on a GCE VM and will explicitly ignore
 * GOOGLE_APPLICATION_CREDENTIALS and use the service account associated with the VM.
 */
@RunWith(JUnit4.class)
public class DirectPathRlsIT {
  @ClassRule
  public static final TestEnvRule testEnvRule = new TestEnvRule();
  @Rule
  public final GrpcCleanupRule grpcRule = new GrpcCleanupRule();
  @Rule
  public Timeout timeout= new Timeout(15, SECONDS);
  private static final String DP_IPV6_PREFIX = "2001:4860:8040";
  private static final String DP_IPV6_GLOBAL_TARGET_PREFIX = "2001:4860:8040:49:0:da";
  private static final String DP_IPV6_GLOBAL_REGIONAL_PREFIX = "2001:4860:8040:2d1:0";
  private static final String DP_IPV4_PREFIX = "34.126";
  private static final String DP_IPV4_GLOBAL_TARGET_PREFIX = "34.126.0";
  private static final String DP_IPV4_GLOBAL_REGIONAL_PREFIX = "34.126.1";
  private static final String LOOKUP_SERVICE = "test-bigtablerls.sandbox.googleapis.com";
  private static final String TARGET = "test-bigtable.sandbox.googleapis.com";
  private static final int DEFAULT_PORT = 443;
  private static final String KEY_MAP_KEY = "x-goog-request-params";
  private static final String READ_ROWS_API_PATH = "/google.bigtable.v2.Bigtable/ReadRows";
  private static final String APP_ID_PROFILE_REGIONAL = "zdapeng-us-east1-b-app-profile";
  private static final String REGIONAL_TARGET = "us-east1-test-bigtable.sandbox.googleapis.com";
  private static final Metadata.Key<String> RLS_DATA_KEY =
      Metadata.Key.of("X-Google-RLS-Data", Metadata.ASCII_STRING_MARSHALLER);
  private static final String GRPC_ENABLE_OOB_CHANNEL_DIRECTPATH_PROPERTY =
      "io.grpc.rls.CachingRlsLbClient.enable_oobchannel_directpath";
  private static final String ROW_KEY = "row-key-dp-rls-test";
  private final AbstractTestEnv testEnv = testEnvRule.env();
  private boolean grpcEnableOobChannelDirectPath;

  @Before
  public void setup() {
    assume()
        .withMessage("DirectPath integration tests can only run against DirectPathEnv")
        .that(testEnvRule.env().isDirectPathEnabled())
        .isTrue();
    grpcEnableOobChannelDirectPath =
        Boolean.getBoolean(GRPC_ENABLE_OOB_CHANNEL_DIRECTPATH_PROPERTY);
    // Populate grpc oobChannel DP service config by RlsChannelResolver for test instead of using
    // default config.
    System.setProperty(GRPC_ENABLE_OOB_CHANNEL_DIRECTPATH_PROPERTY, String.valueOf(false));
    if (!testEnv.getTableAdminClient().exists(testEnv.getTableId())) {
      CreateTableRequest createTableRequest =
          CreateTableRequest.of(testEnv.getTableId()).addFamily(testEnvRule.env().getFamilyId());
      testEnv.getTableAdminClient().createTable(createTableRequest);
      testEnv.getDataClient().mutateRow(
          RowMutation.create(testEnv.getTableId(), ROW_KEY)
              .setCell(testEnvRule.env().getFamilyId(), "qualifier", "value"));
    }
  }

  @After
  public void tearDown() {
    System.setProperty(
        GRPC_ENABLE_OOB_CHANNEL_DIRECTPATH_PROPERTY,
        String.valueOf(grpcEnableOobChannelDirectPath));
  }

  /**
   * Verifies that:
   * <ul>
   *   <li> RLS server is reachable over DP;
   *   <li> RLS server authenticated with overridden authority;
   *   <li> RLS server returns well formed response;
   * </ul>
   */
  @Test
  public void rlsServerWorkingOverDirectPath() {
    ManagedChannel rlsChannel = ComputeEngineChannelBuilder.forAddress(LOOKUP_SERVICE, DEFAULT_PORT)
        .overrideAuthority(TARGET + ":" + DEFAULT_PORT)
        .defaultServiceConfig(getDefaultDirectPathServiceConfig(LOOKUP_SERVICE))
        .disableServiceConfigLookUp()
        .build();
    grpcRule.register(rlsChannel);
    RouteLookupServiceBlockingStub stub = RouteLookupServiceGrpc.newBlockingStub(rlsChannel)
        .withInterceptors(new DirectPathAddressCheckInterceptor())
        .withDeadlineAfter(2, SECONDS);
    String tableName =
        "projects/" + testEnv.getProjectId() + "/instances/" + testEnv.getInstanceId() + "/tables/"
            + testEnv.getTableId();

    // Request without app_profile_id in the Key map
    RouteLookupRequest lookupRequest = RouteLookupRequest.newBuilder()
        .setTargetType("grpc")
        .setPath(READ_ROWS_API_PATH)
        .setServer(TARGET)
        .putKeyMap(KEY_MAP_KEY, "table_name=" + tableName)
        .build();
    RouteLookupResponse response = stub.routeLookup(lookupRequest);
    assertThat(response.getHeaderData()).isEqualTo(TARGET);
    assertThat(response.getTargets(0)).isEqualTo(TARGET);

    // Request with app_profile_id in the Key map
    lookupRequest = RouteLookupRequest.newBuilder()
        .setTargetType("grpc")
        .setPath(READ_ROWS_API_PATH)
        .setServer(TARGET)
        .putKeyMap(
            KEY_MAP_KEY, "table_name=" + tableName + "&app_profile_id=" + APP_ID_PROFILE_REGIONAL)
        .build();
    response = stub.routeLookup(lookupRequest);
    assertThat(response.getHeaderData()).isEqualTo(REGIONAL_TARGET);
    assertThat(response.getTargets(0)).isEqualTo(REGIONAL_TARGET);
  }

  /**
   * Verifies global target is reachable by data client over DP (without RLS).
   */
  @Test
  public void globalTargetConnectableOverDirectPath() throws Exception {
    BigtableDataSettings.Builder settingsBuilder =
        testEnvRule.env().getDataClientSettings().toBuilder();
    InstantiatingGrpcChannelProvider transportProvider =
        (InstantiatingGrpcChannelProvider)
            settingsBuilder.stubSettings().setEndpoint(TARGET + ":" + DEFAULT_PORT)
                .getTransportChannelProvider();
    transportProvider = transportProvider.toBuilder().setChannelConfigurator(
        new ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder>() {
          @Override
          public ManagedChannelBuilder apply(ManagedChannelBuilder builder) {
            builder.intercept(new DirectPathAddressCheckInterceptor());
            return builder;
          }
        }).build();
    settingsBuilder
        .stubSettings()
        .setTransportChannelProvider(transportProvider)
        // Forcefully ignore GOOGLE_APPLICATION_CREDENTIALS
        .setCredentialsProvider(FixedCredentialsProvider.create(ComputeEngineCredentials.create()));
    BigtableDataClient client = BigtableDataClient.create(settingsBuilder.build());
    Row row = client.readRow(testEnv.getTableId(), ROW_KEY);
    assertThat(row.getCells()).hasSize(1);
  }

  /**
   * Verifies regional target is reachable by data client over DP (without RLS).
   */
  @Test
  public void regionalTargetConnectableOverDirectPath() throws Exception {
    BigtableDataSettings.Builder settingsBuilder =
        testEnvRule.env().getDataClientSettings().toBuilder();
    InstantiatingGrpcChannelProvider transportProvider =
        (InstantiatingGrpcChannelProvider)
            settingsBuilder.stubSettings().setEndpoint(REGIONAL_TARGET + ":" + DEFAULT_PORT)
                .getTransportChannelProvider();
    transportProvider = transportProvider.toBuilder().setChannelConfigurator(
        new ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder>() {
          @Override
          public ManagedChannelBuilder apply(ManagedChannelBuilder builder) {
            builder.intercept(new DirectPathAddressCheckInterceptor());
            return builder;
          }
        }).build();
    settingsBuilder
        .stubSettings()
        .setTransportChannelProvider(transportProvider)
        // Forcefully ignore GOOGLE_APPLICATION_CREDENTIALS
        .setCredentialsProvider(FixedCredentialsProvider.create(ComputeEngineCredentials.create()));
    BigtableDataClient client = BigtableDataClient.create(settingsBuilder.build());
    Row row = client.readRow(testEnv.getTableId(), ROW_KEY);
    assertThat(row.getCells()).hasSize(1);
  }

  /**
   * Verifies that:
   * <ul>
   *   <li> Request is routed to global backends;
   *   <li> Global target is connected over DP;
   *   <li> Request headers contains global targets information.
   * </ul>
   */
  @Test
  public void cbtQueryWithRlsLbPolicy_globalTarget() throws Exception {
    DataClientInterceptor testInterceptor = new DataClientInterceptor();
    BigtableDataClient client = buildDataClientWithRlsConfig(
        testInterceptor, new DirectPathAddressCheckInterceptor(), "", null);
    Row row = client.readRow(testEnv.getTableId(), ROW_KEY);
    assertThat(row.getCells()).hasSize(1);
    assertThat(testInterceptor.metadata.get(RLS_DATA_KEY)).isNotEmpty();
    assertThat(testInterceptor.metadata.get(RLS_DATA_KEY))
        .doesNotContain(APP_ID_PROFILE_REGIONAL);
    verifyGlobalTargetAddress(testInterceptor.remoteAddr);
  }

  /**
   * Verifies that:
   * <ul>
   *   <li> Request is routed to regional backends;
   *   <li> Regional target is connected over DP;
   *   <li> Request headers contains regional targets information.
   * </ul>
   */
  @Test
  public void cbtQueryWithRlsLbPolicy_regionalTarget() throws Exception {
    DataClientInterceptor testInterceptor = new DataClientInterceptor();
    BigtableDataClient client = buildDataClientWithRlsConfig(
        testInterceptor, new DirectPathAddressCheckInterceptor(), APP_ID_PROFILE_REGIONAL, null);
    Row row = client.readRow(testEnv.getTableId(), ROW_KEY);
    assertThat(row.getCells()).hasSize(1);
    assertThat(testInterceptor.metadata.get(RLS_DATA_KEY)).contains(REGIONAL_TARGET);
    verifyRegionalTargetAddress(testInterceptor.remoteAddr);
  }

  @Test
  public void rlsError_noDefaultTargetGiven_clientFails() throws Exception {
    DataClientInterceptor testInterceptor = new DataClientInterceptor();
    RlsSuspensionInterceptor rlsSuspensionInterceptor = new RlsSuspensionInterceptor();
    BigtableDataClient client = buildDataClientWithRlsConfig(
        testInterceptor, rlsSuspensionInterceptor, APP_ID_PROFILE_REGIONAL, null);
    ApiFuture<Row> rowFuture = client.readRowAsync(testEnv.getTableId(), ROW_KEY);
    rlsSuspensionInterceptor.failRpc();
    try {
      rowFuture.get(10, SECONDS);
      throw new AssertionError("Data client should fail but not");
    } catch (ExecutionException e) {
      // expected
    }
  }

  @Test
  public void rlsError_defaultTargetGiven_clientFallback() throws Exception {
    DataClientInterceptor testInterceptor = new DataClientInterceptor();
    RlsSuspensionInterceptor rlsSuspensionInterceptor = new RlsSuspensionInterceptor();
    BigtableDataClient client = buildDataClientWithRlsConfig(
        testInterceptor, rlsSuspensionInterceptor, APP_ID_PROFILE_REGIONAL,
        TARGET + ":" + DEFAULT_PORT);
    ApiFuture<Row> rowFuture = client.readRowAsync(testEnv.getTableId(), ROW_KEY);
    rlsSuspensionInterceptor.failRpc();
    assertThat(rowFuture.get(10, SECONDS).getCells()).hasSize(1);
  }

  @Test
  public void rlsNotResponding_noDefaultTargetGiven_clientFails() throws Exception {
    DataClientInterceptor testInterceptor = new DataClientInterceptor();
    BigtableDataClient client = buildDataClientWithRlsConfig(
        testInterceptor, new NoopInterceptor(), APP_ID_PROFILE_REGIONAL, null);
    ApiFuture<Row> rowFuture = client.readRowAsync(testEnv.getTableId(), ROW_KEY);
    try {
      rowFuture.get(10, SECONDS);
      throw new AssertionError("Data client should fail but not");
    } catch (ExecutionException e) {
      // expected
    }
  }

  @Test
  public void rlsNotResponding_defaultTargetGiven_clientFallback() throws Exception {
    DataClientInterceptor testInterceptor = new DataClientInterceptor();
    BigtableDataClient client = buildDataClientWithRlsConfig(
        testInterceptor, new NoopInterceptor(), APP_ID_PROFILE_REGIONAL,
        TARGET + ":" + DEFAULT_PORT);
    ApiFuture<Row> rowFuture = client.readRowAsync(testEnv.getTableId(), ROW_KEY);
    assertThat(rowFuture.get(10, SECONDS).getCells()).hasSize(1);
  }

  @Test
  public void secondQueryUsesCachedRoute() throws Exception {
    DataClientInterceptor testInterceptor = new DataClientInterceptor();
    RlsSuspensionInterceptor rlsSuspensionInterceptor = new RlsSuspensionInterceptor();
    BigtableDataClient client = buildDataClientWithRlsConfig(
        testInterceptor, rlsSuspensionInterceptor, APP_ID_PROFILE_REGIONAL, null);
    ApiFuture<Row> rowFuture = client.readRowAsync(testEnv.getTableId(), ROW_KEY);
    rlsSuspensionInterceptor.completeRpc(); // Complete route lookup
    assertThat(rowFuture.get(10, SECONDS).getCells()).hasSize(1);
    String remoteAddr = ((InetSocketAddress) testInterceptor.remoteAddr).getAddress()
        .getHostAddress();
    // The second request uses cached result so it does not send route lookup request,
    // so rlsSuspensionInterceptor.completeRpc() is not needed.
    Row row = client.readRow(testEnv.getTableId(), ROW_KEY);
    assertThat(row.getCells()).hasSize(1);
    assertThat(((InetSocketAddress) testInterceptor.remoteAddr).getAddress().getHostAddress())
        .isEqualTo(remoteAddr);
  }

  private BigtableDataClient buildDataClientWithRlsConfig(
      final ClientInterceptor dataClientInterceptor,
      final ClientInterceptor routeLookupInterceptor,
      String appProfileId,
      @Nullable String defaultTarget) throws Exception {
    BigtableDataSettings.Builder settingsBuilder =
        testEnvRule.env().getDataClientSettings().toBuilder();
    InstantiatingGrpcChannelProvider transportProvider =
        (InstantiatingGrpcChannelProvider)
            settingsBuilder.stubSettings().getTransportChannelProvider();
    transportProvider = transportProvider.toBuilder()
        .setPoolSize(1) // Use only one channel to test caching behavior.
        .setDirectPathServiceConfig(getRlsServiceConfig(defaultTarget))
        .setChannelConfigurator(
            new ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder>() {
              @Override
              public ManagedChannelBuilder apply(ManagedChannelBuilder builder) {
                return builder
                    .intercept(new DirectPathAddressCheckInterceptor(), dataClientInterceptor)
                    .nameResolverFactory(new RlsChannelResolverFactory(routeLookupInterceptor));
              }
            })
        .build();
    settingsBuilder
        .setAppProfileId(appProfileId)
        .stubSettings()
        .setTransportChannelProvider(transportProvider)
        // Forcefully ignore GOOGLE_APPLICATION_CREDENTIALS
        .setCredentialsProvider(FixedCredentialsProvider.create(ComputeEngineCredentials.create()));
    // disable retry on DEADLINE_EXCEEDED
    settingsBuilder.stubSettings().readRowsSettings().setRetryableCodes(UNAVAILABLE);
    settingsBuilder.stubSettings().readRowSettings().setRetryableCodes(UNAVAILABLE);
    settingsBuilder.stubSettings().bulkReadRowsSettings().setRetryableCodes(UNAVAILABLE);
    return BigtableDataClient.create(settingsBuilder.build());
  }

  private static ImmutableMap<String, ?> getDefaultDirectPathServiceConfig(
      @Nullable String serviceName) {
    ImmutableMap<String, ?> pickFirstStrategy = ImmutableMap.of("pick_first", ImmutableMap.of());
    ImmutableMap<String, ?> childPolicy = serviceName == null
        ? ImmutableMap.of("childPolicy", ImmutableList.of(pickFirstStrategy))
        : ImmutableMap.of(
            "childPolicy", ImmutableList.of(pickFirstStrategy),
            "serviceName", serviceName);
    ImmutableMap<String, ?> grpcLbPolicy = ImmutableMap.of("grpclb", childPolicy);
    return ImmutableMap.of("loadBalancingConfig", ImmutableList.of(grpcLbPolicy));
  }

  private ImmutableMap<String, ?> getRlsServiceConfig(@Nullable String defaultTarget) {
    ImmutableMap<String, ?> pickFirstStrategy = ImmutableMap.of("pick_first", ImmutableMap.of());
    ImmutableMap<String, ?> childPolicy =
        ImmutableMap.of("childPolicy", ImmutableList.of(pickFirstStrategy));
    ImmutableMap<String, ?> grpcLbPolicy = ImmutableMap.of("grpclb", childPolicy);
    ImmutableMap<String, ?> rlsConfig = ImmutableMap.of(
        "routeLookupConfig",
        getLookupConfig(defaultTarget),
        "childPolicy",
        ImmutableList.of(grpcLbPolicy),
        "childPolicyConfigTargetFieldName",
        "serviceName");
    ImmutableMap<String, ?> lbConfig = ImmutableMap.of(
        "rls-experimental", rlsConfig);
    return ImmutableMap.of("loadBalancingConfig", ImmutableList.of(lbConfig));
  }

  private ImmutableMap<String, ?> getLookupConfig(@Nullable String defaultTarget) {
    ImmutableMap<String, ?> grpcKeyBuilders =
        ImmutableMap.of(
            "names",
            ImmutableList.of(
                ImmutableMap.of("service", "google.bigtable.v2.Bigtable")),
            "headers",
            ImmutableList.of(
                ImmutableMap.of(
                    "key",
                    "x-goog-request-params",
                    "names",
                    ImmutableList.of("x-goog-request-params"),
                    "optional",
                    true),
                ImmutableMap.of(
                    "key",
                    "google-cloud-resource-prefix",
                    "names",
                    ImmutableList.of("google-cloud-resource-prefix"),
                    "optional",
                    true)));
    ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<String, Object>()
        .put("grpcKeyBuilders", ImmutableList.of(grpcKeyBuilders))
        .put("lookupService", LOOKUP_SERVICE)
        .put("lookupServiceTimeout", 2D)
        .put("maxAge", 300000D)
        .put("staleAge", 240000D)
        .put(
            "validTargets",
            ImmutableList.of(
                "bigtable.sandbox.googleapis.com",
                "directpath-bigtable.sandbox.googleapis.com"))
        .put("cacheSizeBytes", 1000D)
        .put("requestProcessingStrategy", "SYNC_LOOKUP_DEFAULT_TARGET_ON_ERROR");
    if (defaultTarget != null) {
      builder.put("defaultTarget", defaultTarget);
    }
    return builder.build();
  }

  private static void verifyDpAddress(SocketAddress remoteAddr) {
    String addr = ((InetSocketAddress) remoteAddr).getAddress().getHostAddress();
    assertThat(addr.startsWith(DP_IPV6_PREFIX) || addr.startsWith(DP_IPV4_PREFIX)).isTrue();
  }

  private static void verifyGlobalTargetAddress(SocketAddress remoteAddr) {
    String addr = ((InetSocketAddress) remoteAddr).getAddress().getHostAddress();
    assertThat(addr.startsWith(DP_IPV6_GLOBAL_TARGET_PREFIX)
        || addr.startsWith(DP_IPV4_GLOBAL_TARGET_PREFIX)).isTrue();
  }

  private static void verifyRegionalTargetAddress(SocketAddress remoteAddr) {
    String addr = ((InetSocketAddress) remoteAddr).getAddress().getHostAddress();
    assertThat(addr.startsWith(DP_IPV6_GLOBAL_REGIONAL_PREFIX)
        || addr.startsWith(DP_IPV4_GLOBAL_REGIONAL_PREFIX)).isTrue();
  }

  /**
   * Captures the request attributes "Grpc.TRANSPORT_ATTR_REMOTE_ADDR" when connection is
   * established and verifies if the remote address is a DirectPath address. This is only used for
   * DirectPath testing. {@link ClientCall#getAttributes()}
   */
  private final class DirectPathAddressCheckInterceptor implements ClientInterceptor {
    SocketAddress remoteAddr;

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
      final ClientCall<ReqT, RespT> clientCall = next.newCall(method, callOptions);
      return new SimpleForwardingClientCall<ReqT, RespT>(clientCall) {
        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
          super.start(
              new SimpleForwardingClientCallListener<RespT>(responseListener) {
                @Override
                public void onHeaders(Metadata headers) {
                  // Check peer IP after connection is established.
                  remoteAddr =
                      clientCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
                  verifyDpAddress(remoteAddr);
                  super.onHeaders(headers);
                }
              },
              headers);
        }
      };
    }
  }

  private final class DataClientInterceptor implements ClientInterceptor {
    Metadata metadata;
    SocketAddress remoteAddr;

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
      final ClientCall<ReqT, RespT> clientCall = next.newCall(method, callOptions);
      return new SimpleForwardingClientCall<ReqT, RespT>(clientCall) {
        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
          super.start(
              new SimpleForwardingClientCallListener<RespT>(responseListener) {
                @Override
                public void onHeaders(Metadata metadata) {
                  remoteAddr =
                      clientCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
                  super.onHeaders(metadata);
                }
              },
              headers);
          metadata = headers;
        }
      };
    }
  }

  /** Produces a Name resolver for testing RLS channel. */
  private static final class RlsChannelResolverFactory extends NameResolver.Factory {
    final ClientInterceptor routeLookupInterceptor;

    RlsChannelResolverFactory(ClientInterceptor routeLookupInterceptor) {
      this.routeLookupInterceptor = routeLookupInterceptor;
    }

    @Override
    public NameResolver newNameResolver(URI targetUri, final Args args) {
      NameResolver delegate =
          NameResolverRegistry.getDefaultRegistry().asFactory().newNameResolver(targetUri, args);
      if (delegate == null) {
        return null;
      }
      if (targetUri.toString().contains(LOOKUP_SERVICE)) {
        delegate = new RlsChannelResolver(delegate, args, routeLookupInterceptor);
      }
      return delegate;
    }

    @Override
    public String getDefaultScheme() {
      return "dns";
    }
  }

  private static final class RlsChannelResolver extends NameResolver {
    final NameResolver delegate;
    final Args args;
    final ClientInterceptor routeLookupInterceptor;

    RlsChannelResolver(
        NameResolver delegate, Args args, ClientInterceptor routeLookupInterceptor) {
      this.delegate = delegate;
      this.args = args;
      this.routeLookupInterceptor = routeLookupInterceptor;
    }

    @Override
    public String getServiceAuthority() {
      return delegate.getServiceAuthority();
    }

    @Override
    public void start(final Listener2 listener) {
      Listener2 listener2 = new Listener2() {
        @Override
        public void onResult(ResolutionResult resolutionResult) {
          listener.onResult(
              resolutionResult.toBuilder()
                  .setServiceConfig(
                      args.getServiceConfigParser().parseServiceConfig(
                          getDefaultDirectPathServiceConfig(LOOKUP_SERVICE)))
                  .setAttributes(
                      resolutionResult.getAttributes().toBuilder()
                          .set(InternalConfigSelector.KEY, new ConfigSelector()).build())
                  .build());
        }

        @Override
        public void onError(Status status) {
          listener.onError(status);
        }
      };
      delegate.start(listener2);
    }

    @Override
    public void refresh() {
      delegate.refresh();
    }

    @Override
    public void shutdown() {
      delegate.shutdown();
    }

    final class ConfigSelector extends InternalConfigSelector {

      @Override
      public Result selectConfig(PickSubchannelArgs pickSubchannelArgs) {
        return Result.newBuilder()
            .setConfig(
                args.getServiceConfigParser()
                    .parseServiceConfig(ImmutableMap.<String, Object>of())
                    .getConfig())
            .setInterceptor(routeLookupInterceptor)
            .build();
      }
    }
  }

  /** Suspends RLS RPC until unsuspended or failed. */
  private static final class RlsSuspensionInterceptor implements ClientInterceptor {
    CountDownLatch completionLatch = new CountDownLatch(1);
    Listener<Object> listener;
    Status status;
    Metadata trailers;
    Object respoonse;

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
      return new SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
        @Override
        public void start(Listener<RespT> listener, Metadata metadata) {
          RlsSuspensionInterceptor.this.listener = (Listener<Object>) listener;
          Listener<RespT> callListener = new SimpleForwardingClientCallListener<RespT>(listener) {
            @Override
            public void onMessage(RespT resp) {
              respoonse = resp;
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
              RlsSuspensionInterceptor.this.status = status;
              RlsSuspensionInterceptor.this.trailers = trailers;
              if (!status.isOk()) {
                delegate().onClose(status, trailers);
              }
              completionLatch.countDown();
              // If RPC is successful, suspend RPC completion until either failRpc() or
              // completeRpc() is called.
            }
          };
          delegate().start(callListener, metadata);
        }
      };
    }

    void failRpc() throws InterruptedException {
      completionLatch.await();
      if (status.isOk()) {
        listener.onClose(Status.INTERNAL.withDescription("injected error"), new Metadata());
      }
      completionLatch = new CountDownLatch(1);
    }

    void completeRpc() throws InterruptedException {
      completionLatch.await();
      if (status.isOk()) {
        listener.onMessage(respoonse);
        listener.onClose(status, trailers);
      }
      completionLatch = new CountDownLatch(1);
    }
  }

  /** Interceptor that makes the channel not sending any request. */
  private static final class NoopInterceptor implements ClientInterceptor {

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
      return new SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
        @Override
        public void sendMessage(ReqT message) {
        }

        @Override
        public void request(int numMessages) {
        }

        @Override
        public void halfClose() {
        }
      };
    }
  }
}
