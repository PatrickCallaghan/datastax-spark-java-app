package com.datastax.spark.example;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

public class NetCat {

	private static final int NO_OF_DEVICES = 5000;

	public static void main(String args[]) {
		ServerSocket serverSocket = null;
		Socket clientSocket = null;

		try {
			serverSocket = new ServerSocket(9999);
			clientSocket = serverSocket.accept();
			final PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true); 

			ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
			
			scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
				@Override
				public void run() {
					DateTime time = DateTime.now();

					for (int i = 0; i < NO_OF_DEVICES; i++) {
						String text = String.format("%d;%d;%tQ\n", i, new Double(Math.random() * 20).intValue() + 10,
								time.getMillis());
						System.out.print(text);
						out.write(text);
					}

					out.flush();
				}
			}, 1, 1, TimeUnit.SECONDS);


		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}