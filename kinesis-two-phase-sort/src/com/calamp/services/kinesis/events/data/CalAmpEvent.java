
package com.calamp.services.kinesis.events.data;

import java.io.IOException;
import java.util.Random;

import com.calamp.services.kinesis.events.utils.CalAmpParameters;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Stub for CalAmp LMD message class
 */
public class CalAmpEvent /*extends MessageContent*/{

    private final static ObjectMapper JSON = new ObjectMapper();
    static {
        JSON.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private String ipUdpHeader;
    private String optionsHeader;
    private String messageHeader;
    private String messageContent;

    private long timeStamp;
    private long sequenceNumber;
    private boolean isAck;
    private int machineId;
    
    private Random rand;

    /*This default constructor must exist for JSON serialization*/
    public CalAmpEvent(){}
    
    public CalAmpEvent(String ipUdpHeader, String optionsHeader, String messageHeader, String messageContent) {
    	this.rand = new Random();
    	this.ipUdpHeader = ipUdpHeader;
        this.optionsHeader = optionsHeader;
        this.messageHeader = messageHeader;
        this.messageContent = messageContent;
        
        this.timeStamp = System.currentTimeMillis() + this.rand.nextInt(CalAmpParameters.minimumAgeMillis);
        this.sequenceNumber = this.rand.nextInt(511);
        this.isAck = this.rand.nextBoolean();
        this.machineId = this.rand.nextInt(10000);
    }
    
    public byte[] toJsonAsBytes() {
        try {
            return JSON.writeValueAsBytes(this);
        } catch (IOException e) {
        	e.printStackTrace();
        	return null;
        }
    }

    public static CalAmpEvent fromJsonAsBytes(byte[] bytes) {
        try {
            return JSON.readValue(bytes, CalAmpEvent.class);
        } catch (IOException e) {
        	e.printStackTrace();
            return null;
        }
    }

    private boolean canEqual(Object other){
    	return (other instanceof CalAmpEvent);
    }
    
    @Override
    public String toString() {
        return String.format("Time: %d Seq: %d Ack: %b IpUdpHeader: %s OptionsHeader: %s MessageHeader: %s MessageContent: %s",
                timeStamp, sequenceNumber, isAck, ipUdpHeader, optionsHeader, messageHeader, messageContent);
    }
    
    @Override
    public boolean equals(Object other){
    	if ( this.canEqual(other) ){
    		CalAmpEvent e2 = (CalAmpEvent) other;
    		boolean mayEqual = true;
    		mayEqual &= ( this.timeStamp == e2.getTimeStamp() );
    		mayEqual &= ( this.sequenceNumber == e2.getSequenceNumber() );
    		
    		mayEqual &= ( this.isAck == e2.isAck() ); 
    		mayEqual &= (this.ipUdpHeader == null && e2.ipUdpHeader == null) ? true : ( this.ipUdpHeader.equals(e2.getIpUdpHeader()) );
    		mayEqual &= (this.optionsHeader == null && e2.optionsHeader == null) ? true : ( this.optionsHeader.equals(e2.getOptionsHeader())   );
    		mayEqual &= (this.messageHeader == null && e2.messageHeader == null) ? true : ( this.messageHeader.equals(e2.getMessageHeader()) );
    		mayEqual &= (this.messageContent == null && e2.messageContent == null) ? true : ( this.messageContent.equals(e2.getMessageContent()) );
    		return mayEqual;
    	}
    	return false;
    }
    
    @Override 
    public int hashCode() {
        int result = 0; 
        result += 41 * ( this.timeStamp ^ (this.timeStamp >>> 32));
    	result += 41 * this.sequenceNumber;
    	result += 41 * (this.isAck ? 1 : 0);
        result += 41 * this.machineId;

        result += 41 * ( (this.ipUdpHeader == null) ? 0 : this.ipUdpHeader.hashCode() );
        result += 41 * ( (this.optionsHeader == null) ? 0 : this.optionsHeader.hashCode() );
        result += 41 * ( (this.messageHeader == null) ? 0 : this.messageHeader.hashCode() );
        result += 41 * ( (this.messageContent == null) ? 0 : this.messageContent.hashCode() );
        return result;
    }

	public String getIpUdpHeader() {
		return ipUdpHeader;
	}

	public String getOptionsHeader() {
		return optionsHeader;
	}

	public String getMessageHeader() {
		return messageHeader;
	}

	public String getMessageContent() {
		return messageContent;
	}

	public long getTimeStamp() {
		return timeStamp;
	}

	public long getSequenceNumber() {
		return sequenceNumber;
	}

	public long getMachineId() {
		return sequenceNumber;
	}
	
	public boolean isAck() {
		return isAck;
	}
}
