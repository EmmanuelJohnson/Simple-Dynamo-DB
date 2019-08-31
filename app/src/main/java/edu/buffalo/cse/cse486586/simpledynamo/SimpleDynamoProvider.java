package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.List;
import java.util.concurrent.TimeUnit;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Handler;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;
	static final int [] allPorts = new int[]{11108, 11112, 11116, 11120, 11124};
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	private Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

	private static final List<Node> dynamoRing = new ArrayList<Node>(){{
		add(new Node(5554, 11108));
		add(new Node(5556, 11112));
		add(new Node(5558, 11116));
		add(new Node(5560, 11120));
		add(new Node(5562, 11124));
	}};

	//From PA2A OnPTestClickListener.java File
	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	static String myPort = null;
	static String myAvd = null;
	static Node myNode = null;
	private static boolean isRecovered = false;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		Log.v("delete", selection);
		if(selection.equals("@")){
			//Delete all files in this node
			Log.d("delete", "inside @");
			String[] fileList = getContext().fileList();
			for (int i = 0; i < fileList.length; i++) {
				deleteFile(fileList[i]);
			}
		}
		else if(selection.equals("*")){
			//Delete all files from all nodes
			Log.d("delete", "inside *");
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "", myPort, myAvd, "DeleteAllFiles", "multi");
		}
		else{
			String keyHash = getKeyHash(selection);
			if(myNode.getPredecessor().getHashValue() != null){
				if(dynamoRing.size() == 1 || isBelongToMyNode(keyHash)) {
					deleteFile(selection);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyHash+":"+selection+"#"+myNode.getSuccessor().toString(), myPort, myAvd, "DeleteAtNode", "uni");
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyHash+":"+selection+"#"+myNode.getSuccessor().getSuccessor().toString(), myPort, myAvd, "DeleteAtNode", "uni");
				}
				else{
					Node targetNode = getTargetNode(keyHash);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyHash+":"+selection+"#"+targetNode.toString(), myPort, myAvd, "DeleteAtNode", "uni");
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyHash+":"+selection+"#"+targetNode.getSuccessor().toString(), myPort, myAvd, "DeleteAtNode", "uni");
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyHash+":"+selection+"#"+targetNode.getSuccessor().getSuccessor().toString(), myPort, myAvd, "DeleteAtNode", "uni");
				}
			}
			else{
				deleteFile(selection);
			}
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		while (isRecovered == false){
			Log.d("waitRec", "not yet recovered");
		}
		//From PA2A
		Log.v("insert", values.toString());
		String value = (String) values.get("value");
		String key = (String) values.get("key");
		String keyHash = getKeyHash(key);
		if(myNode.getPredecessor().getHashValue() != null) {
			if (dynamoRing.size() == 1 || isBelongToMyNode(keyHash)) {
				saveFile(key, value);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyHash + ":" + key + ":" + value+"#"+myNode.getSuccessor().toString()+"#"+myNode.toString(), myPort, myAvd, "RepInsertAtNode", "uni");
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyHash + ":" + key + ":" + value+"#"+myNode.getSuccessor().getSuccessor().toString()+"#"+myNode.toString(), myPort, myAvd, "RepInsertAtNode", "uni");
			} else {
				Log.d("check", "look for node: "+key);
				Node targetNode = getTargetNode(keyHash);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyHash + ":" + key + ":" + value+"#"+targetNode.toString(), myPort, myAvd, "InsertAtNode", "uni");
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyHash + ":" + key + ":" + value+"#"+targetNode.getSuccessor().toString()+"#"+targetNode.toString(), myPort, myAvd, "RepInsertAtNode", "uni");
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, keyHash + ":" + key + ":" + value+"#"+targetNode.getSuccessor().getSuccessor().toString()+"#"+targetNode.toString(), myPort, myAvd, "RepInsertAtNode", "uni");
			}
		}
		else{
			saveFile(key, value);
		}

		return uri;
	}

	@Override
	public boolean onCreate() {
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myAvd = portStr;
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		Log.d(TAG, "onCreate: "+myPort);
		myNode = new Node(Integer.parseInt(myAvd), Integer.parseInt(myPort));
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			Log.d(TAG, "onCreate: initialized server socket");
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);//Creating the async task
			Log.d(TAG, "onCreate: initialized a new server task");
		} catch (IOException e) {
			Log.e(TAG, "onCreate: ", e);
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}

		formDynamoRing();
		Log.d("DynamoRing", "###########");
		for (int i = 0; i < dynamoRing.size(); i++) {
			Log.d("DynamoRing", "index:"+i+", "+dynamoRing.get(i).toString());
		}
		new RecoverTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "");
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		//From PA2A
		Log.v("query", selection);                      // The sort order for the returned rows
		//https://stackoverflow.com/questions/18290864/create-a-cursor-from-hardcoded-array-instead-of-db
		String[] columns = new String[] { "key", "value" };
		//https://developer.android.com/reference/android/database/MatrixCursor.html#MatrixCursor(java.lang.String[])
		MatrixCursor cursor = new MatrixCursor(columns);
		if(selection.equals("@")){
			while (isRecovered == false){
				Log.d("waitRec", "not yet recovered");
			}
			//Get all files in this node
			Log.d("query", "inside @");
			synchronized (this){
				FileInputStream inputStream;
				for (int i = 0; i < getContext().fileList().length; i++) {
					try {
						inputStream = getContext().openFileInput(getContext().fileList()[i]);
						BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(inputStream)));
						String line = reader.readLine();
						cursor.addRow(new Object[] {getContext().fileList()[i], line});
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			return cursor;
		}
		else if(selection.equals("*")){
			while (isRecovered == false){
				Log.d("waitRec", "not yet recovered");
			}
			//Get all files from all nodes
			Log.d("query", "inside *");
			int nDevices = 0;
			Message msgObject = new Message("", Integer.parseInt(myPort), Integer.parseInt(myAvd), "GetAllFiles", 1);
			while (nDevices < 5) {
				try {
					int remotePort = allPorts[nDevices];
					msgObject.setToPort(remotePort);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), remotePort);
					//https://stackoverflow.com/questions/5680259/using-sockets-to-send-and-receive-data
					//https://www.careerbless.com/samplecodes/java/beginners/socket/SocketBasic1.php
					OutputStream msgOpStrm = socket.getOutputStream();
					ObjectOutputStream sendOp = new ObjectOutputStream(msgOpStrm);
					sendOp.writeObject(msgObject);
					sendOp.flush();
					//socket.close();
					Message gafRecMsg = null;
					InputStream msgInStrm = socket.getInputStream();
					ObjectInputStream recInp = new ObjectInputStream(msgInStrm);
					gafRecMsg = (Message) recInp.readObject();
					for (int i = 0; i < gafRecMsg.getValList().size(); i++) {
						String keyValPair = gafRecMsg.getValList().get(i).toString();
						cursor.addRow(new Object[] {keyValPair.split(":")[0], keyValPair.split(":")[1]});
					}
					socket.close();
				} catch (UnknownHostException e) {
					Log.e(TAG, "Query * UnknownHostException");
				} catch (IOException e) {
					Log.e(TAG, "Query * socket IOException" + e);
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
				nDevices += 1;
			}
			return cursor;
		}else if(selection.equals("recover")){
			//Get all files from all nodes
			Log.d("query", "inside recover");
			int nDevices = 0;
			Message msgObject = new Message("", Integer.parseInt(myPort), Integer.parseInt(myAvd), "GetAllFiles", 1);
			while (nDevices < 5) {
				if(allPorts[nDevices] == Integer.parseInt(myPort)){
					nDevices += 1;
					continue;
				}
				try {
					int remotePort = allPorts[nDevices];
					msgObject.setToPort(remotePort);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), remotePort);
					//https://stackoverflow.com/questions/5680259/using-sockets-to-send-and-receive-data
					//https://www.careerbless.com/samplecodes/java/beginners/socket/SocketBasic1.php
					OutputStream msgOpStrm = socket.getOutputStream();
					ObjectOutputStream sendOp = new ObjectOutputStream(msgOpStrm);
					sendOp.writeObject(msgObject);
					sendOp.flush();
					//socket.close();
					Message gafRecMsg = null;
					InputStream msgInStrm = socket.getInputStream();
					ObjectInputStream recInp = new ObjectInputStream(msgInStrm);
					gafRecMsg = (Message) recInp.readObject();
					for (int i = 0; i < gafRecMsg.getValList().size(); i++) {
						String keyValPair = gafRecMsg.getValList().get(i).toString();
						cursor.addRow(new Object[] {keyValPair.split(":")[0], keyValPair.split(":")[1]});
					}
					socket.close();
				} catch (UnknownHostException e) {
					Log.e(TAG, "Query * UnknownHostException");
				} catch (IOException e) {
					Log.e(TAG, "Query * socket IOException" + e);
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
				nDevices += 1;
			}
			return cursor;
		}
		else{
			while (isRecovered == false){
				Log.d("waitRec", "not yet recovered");
			}
			String keyHash = getKeyHash(selection);
			Log.d("latest", "################");
			Log.d("latest", "selection: "+selection);
			Node tNode = getTargetNode(keyHash);
			int port1 = tNode.getPort();
			int port2 = tNode.getSuccessor().getPort();
			int port3 = tNode.getSuccessor().getSuccessor().getPort();
			List<Integer> tryList = new ArrayList<Integer>();
			List<String> msgValueList = new ArrayList<String>();
			List<Integer> timeList = new ArrayList<Integer>();
			tryList.add(port3);tryList.add(port2);tryList.add(port1);
			int retry = 0;
			boolean reRetry = false;
			boolean timeout = false;
			while(retry < 3) {
				try {
					Message msgQObject = new Message(keyHash + ":" + selection, Integer.parseInt(myPort), Integer.parseInt(myAvd), "GetQueryAtNode", 8);
					msgQObject.setToPort(tryList.get(retry));
					Socket qSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), msgQObject.getToPort());
					//https://stackoverflow.com/questions/5680259/using-sockets-to-send-and-receive-data
					OutputStream msgQOpStrm = qSocket.getOutputStream();
					ObjectOutputStream sendQOp = new ObjectOutputStream(msgQOpStrm);
					sendQOp.writeObject(msgQObject);
					sendQOp.flush();
					//socket.close();
					Message finalQMsg = null;
					InputStream fMsgInStrm = qSocket.getInputStream();
					ObjectInputStream recQInp = new ObjectInputStream(fMsgInStrm);
					finalQMsg = (Message) recQInp.readObject();
					Log.d("latest", "msg: "+finalQMsg.getMsg()+" and port : "+msgQObject.getToPort());
					if(finalQMsg.getMsg().split(":").length != 3){
						Log.d("latest", "No file");
						if(timeout == false) {
							TimeUnit.SECONDS.sleep(2);
							timeout = true;
						}
						if(retry == 2 && reRetry == false) {
							retry = 0;
							reRetry = true;
							continue;
						}
						retry+=1;
						continue;
					}
					timeList.add(Integer.parseInt(finalQMsg.getMsg().split(";")[0]));
					msgValueList.add(finalQMsg.getMsg());
					qSocket.close();
				} catch (UnknownHostException e) {
					Log.e(TAG, "Query SpecNode UnknownHostException");
				} catch (IOException e) {
					Log.e(TAG, "Query SpecNode socket IOException" + e);
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				retry += 1;
			}
			Log.d("latest", "timelist: "+timeList);
			Collections.sort(msgValueList, Collections.<String>reverseOrder());
			cursor.addRow(new Object[]{msgValueList.get(0).split(":")[1], msgValueList.get(0).split(":")[2]});
			return cursor;
		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	//From PA1
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			try {
				while(true) {
					Socket socket = serverSocket.accept();
					Message recInp = (Message) (new ObjectInputStream(socket.getInputStream())).readObject();
					String recMsg = String.valueOf(recInp.getMsg());
					Log.d(TAG, "message recieved: "+recMsg);
					int msgTypeCode = recInp.getTypeCode();
					switch (msgTypeCode){
						case 1:
							List<String> cVals = new ArrayList<String>();
							try {
								Cursor resultCursor = query(mUri, null, "@", null, null);
								if (resultCursor == null) {
									Log.e(TAG, "Result null");
									throw new Exception();
								}
								resultCursor.moveToFirst();
								for (int i = 0; i < resultCursor.getCount(); i++) {
									String strReceived = resultCursor.getString(resultCursor.getColumnIndex(KEY_FIELD)) + ":"
											+ resultCursor.getString(resultCursor.getColumnIndex(VALUE_FIELD));
									cVals.add(strReceived);
									resultCursor.moveToNext();
								}
							}
							catch (Exception e) {
								Log.e("serverError", "Case 1 Server Error: "+e);
							}
							Message respMsgObj = new Message("GAF-ACK", Integer.parseInt(myPort), Integer.parseInt(myAvd), "RespAllFiles", 2);
							respMsgObj.setValList(cVals);
							respMsgObj.setToPort(recInp.getFromPort());

							OutputStream respMsgOpStrm = socket.getOutputStream();
							ObjectOutputStream respSendOp = new ObjectOutputStream(respMsgOpStrm);
							respSendOp.writeObject(respMsgObj);
							respSendOp.flush();
							break;
						case 2:
							break;
						case 3:
							break;
						case 4:
							Node resNode = recInp.getNode();
							myNode = resNode;
							break;
						case 5:
							break;
						case 6:
							while (isRecovered == false){
								Log.d("waitRec", "not yet recovered");
							}
							//String keyHash = recInp.getMsg().split(":")[0];
							String key = recInp.getMsg().split(":")[1];
							String value = recInp.getMsg().split(":")[2];
							saveFile(key, value);
							break;
						case 7:
							break;
						case 8:
							//String selectionKeyHash = recInp.getMsg().split(":")[0];
							String selectionKey = recInp.getMsg().split(":")[1];
							synchronized (this){
								FileInputStream inputStream;
								try {
//									String timeStamp = Long.toString(getContext().getDir(selectionKey, 'r').lastModified());
									inputStream = getContext().openFileInput(selectionKey);
									BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(inputStream)));
									String line = reader.readLine();
									String version = reader.readLine();
									recInp.setMsg(version+";"+recInp.getMsg()+":"+line);//timeStamp+";"+
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
							OutputStream respQInsMsgOpStrm = socket.getOutputStream();
							ObjectOutputStream respQInsSendOp = new ObjectOutputStream(respQInsMsgOpStrm);
							respQInsSendOp.writeObject(recInp);
							respQInsSendOp.flush();
							break;
						case 9:
							int isDel = delete(mUri, "@", null);
							break;
						case 10:
							break;
						case 11:
							//String dKeyHash = recInp.getMsg().split(":")[0];
							String dKey = recInp.getMsg().split(":")[1];
							Log.d("dcheck", "rmsg: "+recInp.getMsg());
							Log.d("dcheck", "selection: "+dKey);
							deleteFile(dKey);
							break;
						case 12:
							while (isRecovered == false){
								Log.d("waitRec", "not yet recovered");
							}
							String rKey = recInp.getMsg().split(":")[1];
							String rValue = recInp.getMsg().split(":")[2];
							String oPort = recInp.getMsg().split(":")[3];
							saveFile(rKey, rValue);
							break;
						case 200:
							break;
						default:
							break;
					}
					socket.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			return null;
		}

		protected void onProgressUpdate(String... strings) {
			return;
		}
	}

	//From PA1
	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			int nDevices = 0;
			String msgValue = msgs[0];
			int currentPort = Integer.parseInt(msgs[1]);
			int currentAvd = Integer.parseInt(msgs[2]);
			String msgType = msgs[3];
			String multiType = msgs[4];
			Message msgObject = null;
			if(msgType == "GetAllFiles"){
				msgObject = new Message(msgValue, currentPort, currentAvd, msgType, 1);
			}
			else if(msgType == "RespAllFiles"){}
			else if(msgType == "NodeJoinRequest"){
				Log.d("nodejoin", "sending request");
				msgObject = new Message(msgValue, currentPort, currentAvd, msgType, 3);
				//msgObject.setToPort(SUPER_NODE_PORT);
				msgObject.setNode(myNode);
			}
			else if(msgType == "NodeJoinResponse"){
				Log.d("nodejoin", "sending response");
				Node sNode = deSerializeNode(msgValue);
				msgObject = new Message(msgValue, currentPort, currentAvd, msgType, 4);
				msgObject.setToPort(sNode.getPort());
				msgObject.setNode(sNode);
			}
			else if(msgType == "GetNodeLocationInsert"){
				msgObject = new Message(msgValue, currentPort, currentAvd, msgType, 5);
				//msgObject.setToPort(SUPER_NODE_PORT);
			}
			else if(msgType == "InsertAtNode"){
				String[] mSplit = msgValue.split("#");
				msgValue = mSplit[0];
				Node tNode = deSerializeNode(mSplit[1]);
				msgObject = new Message(msgValue, currentPort, currentAvd, msgType, 6);
				msgObject.setToPort(tNode.getPort());
			}
			else if(msgType == "GetNodeLocationQuery"){}
			else if(msgType == "GetQueryAtNode"){}
			else if(msgType == "DeleteAllFiles"){
				msgObject = new Message(msgValue, currentPort, currentAvd, msgType, 9);
			}
			else if(msgType == "FindAndDeleteAtNode"){
				msgObject = new Message(msgValue, currentPort, currentAvd, msgType, 10);
				//msgObject.setToPort(SUPER_NODE_PORT);
			}
			else if(msgType == "DeleteAtNode"){
				String[] mSplit = msgValue.split("#");
				msgValue = mSplit[0];
				Node tdNode = deSerializeNode(mSplit[1]);
				msgObject = new Message(msgValue, currentPort, currentAvd, msgType, 11);
				msgObject.setToPort(tdNode.getPort());
			}
			else if(msgType == "RepInsertAtNode"){
				String[] mSplit = msgValue.split("#");
				Node tNode = deSerializeNode(mSplit[1]);
				Node cNode = deSerializeNode(mSplit[2]);
				msgValue = mSplit[0]+":"+cNode.getPort();
				msgObject = new Message(msgValue, currentPort, currentAvd, msgType, 12);
				msgObject.setToPort(tNode.getPort());
			}
			if(multiType == "multi") {
				while (nDevices < 5) {
					try {
						int remotePort = allPorts[nDevices];
						msgObject.setToPort(remotePort);
						Socket multiSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), remotePort);
						//https://stackoverflow.com/questions/5680259/using-sockets-to-send-and-receive-data
						OutputStream msgOpStrm = multiSocket.getOutputStream();
						ObjectOutputStream sendOp = new ObjectOutputStream(msgOpStrm);
						sendOp.writeObject(msgObject);
						sendOp.flush();
						multiSocket.close();
					} catch (UnknownHostException e) {
						Log.e(TAG, "ClientTask UnknownHostException");
					} catch (IOException e) {
						Log.e(TAG, "ClientTask socket IOException" + e);
					}
					nDevices += 1;
				}
			}
			else if(multiType == "uni"){
				try {
					Socket uniSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), msgObject.getToPort());
					OutputStream msgOpStrm = uniSocket.getOutputStream();
					ObjectOutputStream sendOp = new ObjectOutputStream(msgOpStrm);
					sendOp.writeObject(msgObject);
					sendOp.flush();
					uniSocket.close();
				} catch (UnknownHostException e) {
					Log.e("ServerError", "ClientTask UnknownHostException");
				} catch (IOException e) {
					Log.e("ServerError", "ClientTask socket IOException"+e);
				}
			}
			return null;
		}
	}

	private class RecoverTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... args) {
			if(isRecovered == false){
				try {
					recoverValues();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			isRecovered = true;
			return null;
		}
	}

	public void recoverValues() throws Exception {
		delete(mUri, "@", null);
		Cursor allFiles = query(mUri, null, "recover", null, null);
		if (allFiles == null) {
			Log.e(TAG, "Result null");
			throw new Exception();
		}
		Node pred1 = myNode.getPredecessor();
		Node pred2 = myNode.getPredecessor().getPredecessor();
		Log.d("recovery", "pred1 port: "+pred1.getAvd()+" : pred1 pred: "+pred1.getPredecessor().getAvd());
		Log.d("recovery", "pred2 port: "+pred2.getAvd()+" : pred2 pred: "+pred2.getPredecessor().getAvd());
		allFiles.moveToFirst();
		for (int i = 0; i < allFiles.getCount(); i++) {
			String key = allFiles.getString(allFiles.getColumnIndex(KEY_FIELD));
			String value = allFiles.getString(allFiles.getColumnIndex(VALUE_FIELD));

			String keyHash = getKeyHash(key);
			Node targetNode = getTargetNode(keyHash);
			if (targetNode.getAvd() == myNode.getAvd()) {
				Log.d("recovery", "this node key: "+key+" : value: "+value);
				File file = getContext().getFileStreamPath(key);
				if(file == null || !file.exists()) {
					saveFile(key, value);
				}
			}
			else if(targetNode.getAvd() == pred1.getAvd()){
				Log.d("recovery", "predecessor 1 key: "+key+" : value: "+value);
//				addReplicationData(Integer.toString(pred1.getPort()), key+":"+value);
				saveFile(key, value);
			}
			else if(targetNode.getAvd() == pred2.getAvd()){
				Log.d("recovery", "predecessor 2 key: "+key+" : value: "+value);
//				addReplicationData(Integer.toString(pred2.getPort()), key+":"+value);
				saveFile(key, value);
			}
			else{
				Log.d("recovery", "None key: "+key+" : value: "+value);
			}
			allFiles.moveToNext();
		}
	}

	public void formDynamoRing(){
		Collections.sort(dynamoRing);
		for (int i = 0; i < dynamoRing.size(); i++) {
			Node cNode = dynamoRing.get(i);
			if(i == 0){
				cNode.setPredecessor(dynamoRing.get(dynamoRing.size()-1));
				if(dynamoRing.size() == 1)
					cNode.setSuccessor(dynamoRing.get(0));
				else
					cNode.setSuccessor(dynamoRing.get(i+1));
			}
			else if(i > 0 && i < dynamoRing.size()-1){
				cNode.setPredecessor(dynamoRing.get(i-1));
				cNode.setSuccessor(dynamoRing.get(i+1));
			}
			else{
				cNode.setPredecessor(dynamoRing.get(i-1));
				cNode.setSuccessor(dynamoRing.get(0));
			}
			if(cNode.getAvd() == Integer.parseInt(myAvd)){
				myNode = cNode;
			}
		}
	}

	public synchronized Node getTargetNode(String keyHash){
		Node tmpQNode = new Node(Integer.parseInt(myAvd), Integer.parseInt(myPort));
		tmpQNode.setHashValue(keyHash);
		dynamoRing.add(tmpQNode);
		Collections.sort(dynamoRing);
		int targetQIndex;
		if(dynamoRing.indexOf(tmpQNode) == dynamoRing.size()-1){
			targetQIndex = 0;
		}
		else{
			targetQIndex = dynamoRing.indexOf(tmpQNode) + 1;
		}
		Node targetNode = dynamoRing.get(targetQIndex);
		dynamoRing.remove(tmpQNode);
		Collections.sort(dynamoRing);
		return targetNode;
	}

	public Node deSerializeNode(String node){
		String[] nSplit = node.split(":");
		Node dNode = new Node(Integer.parseInt(nSplit[1]), Integer.parseInt(nSplit[0]));
		dNode.setHashValue(nSplit[2]);
		return dNode;
	}

	public String getKeyHash(String key){
		String keyHash = null;
		try {
			keyHash = genHash(key);
		}
		catch (NoSuchAlgorithmException nsae){
			Log.e("DeleteKeyHash", "delete: "+nsae);
		}
		return keyHash;
	}

	//http://www.lucazanini.eu/en/2016/android/saving-reading-files-internal-storage/
	public synchronized void saveFile(String key, String value){
		Log.d("database", "key: "+key+" : value: "+value);
		int version;
		FileOutputStream outputStream;
		try {
			File file = getContext().getFileStreamPath(key);
			if(file == null || !file.exists()) {
				version = 1;
			}
			else{
				FileInputStream inputStream = getContext().openFileInput(key);
				BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(inputStream)));
				String line = reader.readLine();
				String versionValue = reader.readLine();
				if(line == value)
					version = 1;
				else
					version = Integer.parseInt(versionValue)+1;
			}
			outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
			outputStream.write(value.getBytes());
			outputStream.write("\n".getBytes());
			outputStream.write(Integer.toString(version).getBytes());
			outputStream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void deleteFile(String fileName){
		Log.d("database", "Deleting key: "+fileName);
		try {
			getContext().deleteFile(fileName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public boolean isBelongToMyNode(String keyHash){
		List<String> tmpList = Arrays.asList(myNode.getPredecessor().getHashValue(), keyHash, myNode.getHashValue());
		Collections.sort(tmpList);
		if (tmpList.get(0) == myNode.getPredecessor().getHashValue() && tmpList.get(1) == keyHash && tmpList.get(2) == myNode.getHashValue()){
			return true;
		}
		return false;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}
}

class Node implements Comparable<Node>, Serializable {
	private int port;
	private int avd;
	private String hashValue;
	private Node successor;
	private Node predecessor;

	public Node(int avd, int port) {
		this.port = port;
		this.avd = avd;
		try {
			this.hashValue = genHash(String.valueOf(avd));
		}
		catch (NoSuchAlgorithmException nsae){
			Log.e("NodeConstructorError", "Node: "+nsae);
		}
	}

	public String getHashValue(){
		return this.hashValue;
	}

	public int getPort(){
		return this.port;
	}

	public int getAvd(){
		return this.avd;
	}

	public Node getSuccessor(){
		return this.successor;
	}

	public Node getPredecessor(){
		return this.predecessor;
	}

	public void setHashValue(String hv){
		this.hashValue = hv;
	}

	public void setSuccessor(Node succ){
		this.successor = succ;
	}

	public void setPredecessor(Node pred){
		this.predecessor = pred;
	}

	public String toString(){
		return String.valueOf(this.port)+":"+String.valueOf(this.avd)+":"+this.getHashValue()+":"+String.valueOf(this.predecessor.getHashValue())+":"+String.valueOf(this.successor.getHashValue());
	}

	@Override
	public int compareTo(Node n) {
		return this.getHashValue().compareTo(n.getHashValue());
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}
}

//Class to send the message as an object during multicast
class Message implements Serializable {

	private String msg;
	private int fromPort;
	private int toPort;
	private int avd;
	private String type;
	private int typeCode;
	private List<String> valList;
	private Node node;

	public Message(String msg, int port, int avd, String type, int typeCode){
		this.msg = msg;
		this.fromPort = port;
		this.avd = avd;
		this.type = type;
		this.typeCode = typeCode;
	}

	public String getMsg(){
		return this.msg;
	}

	public int getFromPort(){
		return this.fromPort;
	}

	public int getToPort(){
		return this.toPort;
	}

	public int getAvd(){
		return this.avd;
	}

	public String getType(){
		return this.type;
	}

	public int getTypeCode(){
		return this.typeCode;
	}

	public List getValList(){ return this.valList; }

	public Node getNode(){
		return this.node;
	}

	public void setMsg(String msg){
		this.msg = msg;
	}

	public void setToPort(int port){ this.toPort = port; }

	public void setValList(List<String> values){
		this.valList = values;
	}

	public void setNode(Node n){
		this.node = n;
	}
}