package darkhipo;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesis;

public class LazyLogger {

	public static void log(String fullPath, Boolean doAppend, String message) {
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(fullPath, doAppend));
			bw.write( ts() + "# " + message );
			bw.newLine();
			bw.flush();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally { 
			if (bw != null)
				try {
					bw.close();
				} catch (IOException ioe2) {
					ioe2.printStackTrace();
				}
		} 
	}
	
	public static String ts(){
		Date currentTime = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("ss:mm:hh:d:MM:yyyy");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC")); //GMT UTC
		return( sdf.format(currentTime) );
	}
}
