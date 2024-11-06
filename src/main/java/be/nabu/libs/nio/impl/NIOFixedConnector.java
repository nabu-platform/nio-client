/*
* Copyright (C) 2017 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

package be.nabu.libs.nio.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import be.nabu.libs.nio.api.NIOClient;
import be.nabu.libs.nio.api.NIOConnector;
import be.nabu.libs.nio.api.Pipeline;

public class NIOFixedConnector implements NIOConnector {

	private int fixedPort;
	private String fixedHost;

	public NIOFixedConnector(String fixedHost, int fixedPort) {
		this.fixedHost = fixedHost;
		this.fixedPort = fixedPort;
	}
	
	@Override
	public SocketChannel connect(NIOClient client, String host, Integer port) throws IOException {
		InetSocketAddress serverAddress = new InetSocketAddress(fixedHost, fixedPort);
		SocketChannel channel = SocketChannel.open();
	    channel.configureBlocking(false);
	    channel.connect(serverAddress);
	    return channel;
	}

	@Override
	public void tunnel(NIOClient client, String host, Integer port, Pipeline pipeline) throws IOException {
		// do nothing
	}

}
