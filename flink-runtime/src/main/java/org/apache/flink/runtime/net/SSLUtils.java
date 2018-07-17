/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.net;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.TrustManagerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.net.ServerSocket;
import java.security.KeyStore;
import java.util.Arrays;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Common utilities to manage SSL transport settings.
 */
public class SSLUtils {
	private static final Logger LOG = LoggerFactory.getLogger(SSLUtils.class);

	/**
	 * Retrieves the global ssl flag from configuration.
	 *
	 * @param sslConfig
	 *        The application configuration
	 * @return true if global ssl flag is set
	 */
	public static boolean getSSLEnabled(Configuration sslConfig) {

		Preconditions.checkNotNull(sslConfig);

		return sslConfig.getBoolean(SecurityOptions.SSL_ENABLED);
	}

	/**
	 * Sets SSl version and cipher suites for SSLServerSocket.
	 * @param socket
	 *        Socket to be handled
	 * @param config
	 *        The application configuration
	 */
	public static void setSSLVerAndCipherSuites(ServerSocket socket, Configuration config) {
		if (socket instanceof SSLServerSocket) {
			final String[] protocols = config.getString(SecurityOptions.SSL_PROTOCOL).split(",");

			final String[] cipherSuites = config.getString(SecurityOptions.SSL_ALGORITHMS).split(",");

			if (LOG.isDebugEnabled()) {
				LOG.debug("Configuring TLS version and cipher suites on SSL socket {} / {}",
						Arrays.toString(protocols), Arrays.toString(cipherSuites));
			}

			((SSLServerSocket) socket).setEnabledProtocols(protocols);
			((SSLServerSocket) socket).setEnabledCipherSuites(cipherSuites);
		}
	}

	/**
	 * Creates a {@link SSLEngineFactory} to be used by the Server.
	 *
	 * @param config The application configuration.
	 */
	public static SSLEngineFactory createServerSSLEngineFactory(final Configuration config) throws Exception {
		return createSSLEngineFactory(config, false);
	}

	/**
	 * Creates a {@link SSLEngineFactory} to be used by the Client.
	 * @param config The application configuration.
	 */
	public static SSLEngineFactory createClientSSLEngineFactory(final Configuration config) throws Exception {
		return createSSLEngineFactory(config, true);
	}

	private static SSLEngineFactory createSSLEngineFactory(
			final Configuration config,
			final boolean clientMode) throws Exception {

		final SSLContext sslContext = clientMode ?
			createSSLClientContext(config) :
			createSSLServerContext(config);

		checkState(sslContext != null, "%s it not enabled", SecurityOptions.SSL_ENABLED.key());

		return new SSLEngineFactory(
			sslContext,
			getEnabledProtocols(config),
			getEnabledCipherSuites(config),
			clientMode);
	}

	/**
	 * Sets SSL version and cipher suites for SSLEngine.
	 *
	 * @param engine SSLEngine to be handled
	 * @param config The application configuration
	 * @deprecated Use {@link #createClientSSLEngineFactory(Configuration)} or
	 * {@link #createServerSSLEngineFactory(Configuration)}.
	 */
	@Deprecated
	public static void setSSLVerAndCipherSuites(SSLEngine engine, Configuration config) {
		engine.setEnabledProtocols(getEnabledProtocols(config));
		engine.setEnabledCipherSuites(getEnabledCipherSuites(config));
	}

	private static String[] getEnabledProtocols(final Configuration config) {
		requireNonNull(config, "config must not be null");
		return config.getString(SecurityOptions.SSL_PROTOCOL).split(",");
	}

	private static String[] getEnabledCipherSuites(final Configuration config) {
		requireNonNull(config, "config must not be null");
		return config.getString(SecurityOptions.SSL_ALGORITHMS).split(",");
	}

	/**
	 * Sets SSL options to verify peer's hostname in the certificate.
	 *
	 * @param sslConfig
	 *        The application configuration
	 * @param sslParams
	 *        The SSL parameters that need to be updated
	 */
	public static void setSSLVerifyHostname(Configuration sslConfig, SSLParameters sslParams) {

		Preconditions.checkNotNull(sslConfig);
		Preconditions.checkNotNull(sslParams);

		boolean verifyHostname = sslConfig.getBoolean(SecurityOptions.SSL_VERIFY_HOSTNAME);
		if (verifyHostname) {
			sslParams.setEndpointIdentificationAlgorithm("HTTPS");
		}
	}

	/**
	 * Configuration settings and key/trustmanager instances to set up an SSL client connection.
	 */
	public static class SSLClientConfiguration {
		public final String sslProtocolVersion;
		public final TrustManagerFactory trustManagerFactory;
		public final int sessionCacheSize;
		public final int sessionTimeoutMs;
		public final int handshakeTimeoutMs;
		public final int closeNotifyFlushTimeoutMs;

		public SSLClientConfiguration(
				String sslProtocolVersion,
				TrustManagerFactory trustManagerFactory,
				int sessionCacheSize,
				int sessionTimeoutMs,
				int handshakeTimeoutMs,
				int closeNotifyFlushTimeoutMs) {
			this.sslProtocolVersion = sslProtocolVersion;
			this.trustManagerFactory = trustManagerFactory;
			this.sessionCacheSize = sessionCacheSize;
			this.sessionTimeoutMs = sessionTimeoutMs;
			this.handshakeTimeoutMs = handshakeTimeoutMs;
			this.closeNotifyFlushTimeoutMs = closeNotifyFlushTimeoutMs;
		}
	}

	/**
	 * Creates necessary helper objects to use for creating an SSL Context for the client if SSL is
	 * configured.
	 *
	 * @param sslConfig
	 *        The application configuration
	 * @return The SSLClientConfiguration object which can be used for creating some SSL context object;
	 * 	       returns <tt>null</tt> if SSL is disabled.
	 * @throws Exception
	 *         Thrown if there is any misconfiguration
	 */
	@Nullable
	public static SSLClientConfiguration createSSLClientConfiguration(Configuration sslConfig) throws Exception {
		Preconditions.checkNotNull(sslConfig);

		if (getSSLEnabled(sslConfig)) {
			LOG.debug("Creating client SSL configuration");

			String trustStoreFilePath = sslConfig.getString(SecurityOptions.SSL_TRUSTSTORE);
			String trustStorePassword = sslConfig.getString(SecurityOptions.SSL_TRUSTSTORE_PASSWORD);
			String sslProtocolVersion = sslConfig.getString(SecurityOptions.SSL_PROTOCOL);
			int sessionCacheSize = sslConfig.getInteger(SecurityOptions.SSL_SESSION_CACHE_SIZE);
			int sessionTimeoutMs = sslConfig.getInteger(SecurityOptions.SSL_SESSION_TIMEOUT);
			int handshakeTimeoutMs = sslConfig.getInteger(SecurityOptions.SSL_HANDSHAKE_TIMEOUT);
			int closeNotifyFlushTimeoutMs = sslConfig.getInteger(SecurityOptions.SSL_CLOSE_NOTIFY_FLUSH_TIMEOUT);

			Preconditions.checkNotNull(trustStoreFilePath, SecurityOptions.SSL_TRUSTSTORE.key() + " was not configured.");
			Preconditions.checkNotNull(trustStorePassword, SecurityOptions.SSL_TRUSTSTORE_PASSWORD.key() + " was not configured.");

			KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());

			try (FileInputStream trustStoreFile = new FileInputStream(new File(trustStoreFilePath))) {
				trustStore.load(trustStoreFile, trustStorePassword.toCharArray());
			}

			TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
				TrustManagerFactory.getDefaultAlgorithm());
			trustManagerFactory.init(trustStore);

			return new SSLClientConfiguration(
				sslProtocolVersion,
				trustManagerFactory,
				sessionCacheSize,
				sessionTimeoutMs,
				handshakeTimeoutMs,
				closeNotifyFlushTimeoutMs);
		}

		return null;
	}

	/**
	 * Creates the SSL Context for the client assuming SSL is configured.
	 *
	 * @param sslConfig
	 *        SSL configuration
	 * @return The SSLContext object which can be used by the ssl transport client
	 * @throws Exception
	 *         Thrown if there is any misconfiguration
	 */
	public static SSLContext createSSLClientContext(SSLClientConfiguration sslConfig) throws Exception {
		Preconditions.checkNotNull(sslConfig);

		LOG.debug("Creating client SSL context from configuration");
		SSLContext clientSSLContext = SSLContext.getInstance(sslConfig.sslProtocolVersion);
		clientSSLContext.init(null, sslConfig.trustManagerFactory.getTrustManagers(), null);
		if (sslConfig.sessionCacheSize >= 0) {
			clientSSLContext.getClientSessionContext().setSessionCacheSize(sslConfig.sessionCacheSize);
		}
		if (sslConfig.sessionTimeoutMs >= 0) {
			clientSSLContext.getClientSessionContext().setSessionTimeout(sslConfig.sessionTimeoutMs / 1000);
		}

		return clientSSLContext;
	}

	/**
	 * Creates the SSL Context for the client if SSL is configured.
	 *
	 * @param sslConfig
	 *        The application configuration
	 * @return The SSLContext object which can be used by the ssl transport client
	 * 	       Returns null if SSL is disabled
	 * @throws Exception
	 *         Thrown if there is any misconfiguration
	 */
	@Nullable
	public static SSLContext createSSLClientContext(Configuration sslConfig) throws Exception {
		Preconditions.checkNotNull(sslConfig);
		SSLContext clientSSLContext = null;

		if (getSSLEnabled(sslConfig)) {
			SSLClientConfiguration clientConfiguration = createSSLClientConfiguration(sslConfig);
			clientSSLContext = createSSLClientContext(clientConfiguration);
		}

		return clientSSLContext;
	}

	/**
	 * Configuration settings and key/trustmanager instances to set up an SSL server connection.
	 */
	public static class SSLServerConfiguration {
		public final String sslProtocolVersion;
		public final String[] ciphers;
		public final KeyManagerFactory keyManagerFactory;
		public final int sessionCacheSize;
		public final int sessionTimeoutMs;
		public final int handshakeTimeoutMs;
		public final int closeNotifyFlushTimeoutMs;

		public SSLServerConfiguration(
				String sslProtocolVersion,
				String[] ciphers,
				KeyManagerFactory keyManagerFactory,
				int sessionCacheSize,
				int sessionTimeoutMs,
				int handshakeTimeoutMs,
				int closeNotifyFlushTimeoutMs) {
			this.sslProtocolVersion = sslProtocolVersion;
			this.ciphers = ciphers;
			this.keyManagerFactory = keyManagerFactory;
			this.sessionCacheSize = sessionCacheSize;
			this.sessionTimeoutMs = sessionTimeoutMs;
			this.handshakeTimeoutMs = handshakeTimeoutMs;
			this.closeNotifyFlushTimeoutMs = closeNotifyFlushTimeoutMs;
		}
	}

	/**
	 * Creates necessary helper objects to use for creating an SSL Context for the server if SSL is
	 * configured.
	 *
	 * @param sslConfig
	 *        The application configuration
	 * @return The SSLServerConfiguration object which can be used for creating some SSL context object;
	 * 	       returns <tt>null</tt> if SSL is disabled.
	 * @throws Exception
	 *         Thrown if there is any misconfiguration
	 */
	@Nullable
	public static SSLServerConfiguration createSSLServerConfiguration(Configuration sslConfig) throws Exception {
		Preconditions.checkNotNull(sslConfig);

		if (getSSLEnabled(sslConfig)) {
			LOG.debug("Creating server SSL configuration");

			String keystoreFilePath = sslConfig.getString(SecurityOptions.SSL_KEYSTORE);
			String keystorePassword = sslConfig.getString(SecurityOptions.SSL_KEYSTORE_PASSWORD);
			String certPassword = sslConfig.getString(SecurityOptions.SSL_KEY_PASSWORD);
			String sslProtocolVersion = sslConfig.getString(SecurityOptions.SSL_PROTOCOL);
			String[] sslCipherSuites = sslConfig.getString(SecurityOptions.SSL_ALGORITHMS).split(",");
			int sessionCacheSize = sslConfig.getInteger(SecurityOptions.SSL_SESSION_CACHE_SIZE);
			int sessionTimeoutMs = sslConfig.getInteger(SecurityOptions.SSL_SESSION_TIMEOUT);
			int handshakeTimeoutMs = sslConfig.getInteger(SecurityOptions.SSL_HANDSHAKE_TIMEOUT);
			int closeNotifyFlushTimeoutMs = sslConfig.getInteger(SecurityOptions.SSL_CLOSE_NOTIFY_FLUSH_TIMEOUT);

			Preconditions.checkNotNull(keystoreFilePath, SecurityOptions.SSL_KEYSTORE.key() + " was not configured.");
			Preconditions.checkNotNull(keystorePassword, SecurityOptions.SSL_KEYSTORE_PASSWORD.key() + " was not configured.");
			Preconditions.checkNotNull(certPassword, SecurityOptions.SSL_KEY_PASSWORD.key() + " was not configured.");

			KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
			try (FileInputStream keyStoreFile = new FileInputStream(new File(keystoreFilePath))) {
				ks.load(keyStoreFile, keystorePassword.toCharArray());
			}

			// Set up key manager factory to use the server key store
			KeyManagerFactory kmf = KeyManagerFactory.getInstance(
				KeyManagerFactory.getDefaultAlgorithm());
			kmf.init(ks, certPassword.toCharArray());

			return new SSLServerConfiguration(
				sslProtocolVersion,
				sslCipherSuites,
				kmf,
				sessionCacheSize,
				sessionTimeoutMs,
				handshakeTimeoutMs,
				closeNotifyFlushTimeoutMs);
		}

		return null;
	}

	/**
	 * Creates the SSL Context for the server assuming SSL is configured.
	 *
	 * @param sslConfig
	 *        The application configuration
	 * @return The SSLContext object which can be used by the ssl transport server
	 * @throws Exception
	 *         Thrown if there is any misconfiguration
	 */
	@Nullable
	public static SSLContext createSSLServerContext(SSLServerConfiguration sslConfig) throws Exception {
		Preconditions.checkNotNull(sslConfig);

		LOG.debug("Creating server SSL context from configuration");
		SSLContext serverSSLContext = SSLContext.getInstance(sslConfig.sslProtocolVersion);
		serverSSLContext.init(sslConfig.keyManagerFactory.getKeyManagers(), null, null);
		if (sslConfig.sessionCacheSize >= 0) {
			serverSSLContext.getServerSessionContext().setSessionCacheSize(sslConfig.sessionCacheSize);
		}
		if (sslConfig.sessionTimeoutMs >= 0) {
			serverSSLContext.getServerSessionContext().setSessionTimeout(sslConfig.sessionTimeoutMs / 1000);
		}

		return serverSSLContext;
	}

	/**
	 * Creates the SSL Context for the server if SSL is configured.
	 *
	 * @param sslConfig
	 *        The application configuration
	 * @return The SSLContext object which can be used by the ssl transport server
	 * 	       Returns null if SSL is disabled
	 * @throws Exception
	 *         Thrown if there is any misconfiguration
	 */
	@Nullable
	public static SSLContext createSSLServerContext(Configuration sslConfig) throws Exception {

		Preconditions.checkNotNull(sslConfig);
		SSLContext serverSSLContext = null;

		if (getSSLEnabled(sslConfig)) {
			SSLServerConfiguration serverTools = createSSLServerConfiguration(sslConfig);

			// Initialize the SSLContext
			serverSSLContext = SSLContext.getInstance(serverTools.sslProtocolVersion);
			serverSSLContext.init(serverTools.keyManagerFactory.getKeyManagers(), null, null);
		}

		return serverSSLContext;
	}

}
