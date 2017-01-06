import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class GPSRecordEvent implements Interceptor {
	private Schema schema = null; 
	private static String messageName = "gpsrecord"; //CHANGE this name for every topic
	
	
	//Schema url is attached to every event with a static interceptor.
	//getSchema gets an event as input.
	//It extracts the url from the header with the key: flume.avro.schema.url
	private Schema getSchema(Event event) {
    	//Schema URL is attached by static interceptor in event header. 
    	//This string is extracted by the get method on event
    	String schemaFile = (String) event.getHeaders().get("flume.avro.schema.url");
    	
    	//Create and initialize Schema var. This is set to null by default. Otherwise there is 
    	//a possibility it's undefined after the try except.
    	Schema schema = null;
    	try {
    		//Schemafile is an url pointing to the right schema. 
    		//Open this schema is a file and parse this file.
    		//The result (avro schema)  is put in schema.
    		schema = new Schema.Parser().parse(new File(schemaFile));
    	}
    	catch (IOException ex) {
    		ex.printStackTrace();
    	}
    	
    	return schema;
	}

	//Function receives an event. This event has a JSON Message in its body.
	//This JSON message is represented as byte[] and finally returned as a JSON Object.
	//This makes it easier to extract info from it.
	private JsonObject getJSON(Event event) {
		//Original message is stored in event body as an array of bytes/
		//Array of bytes is converted to String.
		String jsonString = new String(event.getBody());
		
		//Create JsonParser used for parsing String to JSONObject.
		JsonParser parser = new JsonParser();
		//Extract a jsonObject from the input string. This string represent the JSON format.
		JsonObject jsonObject = parser.parse(jsonString).getAsJsonObject();
		
		//Every JSON message has one substructure where the name is the same as the JSON message. All datafields belong to this substructure.
		//Hive will represent this as one big struct(Single column). We wont every fieldname to be in one column. There for we extract all these fieldnames.
		//By taking the content of this fieldname (this.messageName) and represent its content as a JSON Object.
		return jsonObject.get(this.messageName).getAsJsonObject();
	}
	
	//getAvroRecord extracts data from the JSON object and puts it in a GenericRecord.
	//This GenericRecord is based on the the corresponding AVRO schema for this message.
	//GenericRecord is filled with corresponding values in JSON message.
	//Some fields in genericrecords are optional. These field are only filled when present in the JSON messages.
	//Otherwise these fields are: null.
	private GenericRecord getAvroRecord(JsonObject jsonData) {
		GenericRecord gpsRecord = new GenericData.Record(this.schema);
	
		//Accessclass is optional
		if (jsonData.has("accessclass")) {
			gpsRecord.put("accessclass", jsonData.get("accessclass").getAsInt());
		}

		//Accessid
		gpsRecord.put("accessid", jsonData.get("accessid").getAsLong());
		
		//Accessnetwork
		gpsRecord.put("accessnetwork", jsonData.get("accessnetwork").getAsInt());
		
		//BeamID
		gpsRecord.put("beamid", jsonData.get("beamid").getAsInt());
		
		//options is optional
		GenericRecord options = new GenericData.Record(this.schema.getField("options").schema());
		//Only if it has the field options in the JSON message.
		if (jsonData.has("options")) {
			JsonObject jsonOptions = jsonData.get("options").getAsJsonObject();
			options.put("elevationband", jsonOptions.get("elevationband").getAsInt());
		}
		gpsRecord.put("options", options);
		
		//Position
		GenericRecord position = new GenericData.Record(this.schema.getField("position").schema());
		JsonObject jsonPosition = jsonData.get("position").getAsJsonObject();
		
			//Altitude Optional
			GenericRecord altitude = new GenericData.Record(this.schema.getField("position").schema().getField("altitude").schema());
			if (jsonPosition.has("altitude")) {
				JsonObject jsonAltitude = jsonPosition.get("altitude").getAsJsonObject();
				altitude.put("height", jsonAltitude.get("height").getAsDouble());
			}
			position.put("altitude", altitude);
			
			//Latitude
			GenericRecord latitude = new GenericData.Record(this.schema.getField("position").schema().getField("latitude").schema());
			latitude.put("position", jsonPosition.get("latitude").getAsJsonObject().get("position").getAsDouble());
			latitude.put("sense", jsonPosition.get("latitude").getAsJsonObject().get("sense").getAsString());
			position.put("latitude", latitude);
			
			//Longitude
			GenericRecord longitude = new GenericData.Record(this.schema.getField("position").schema().getField("longitude").schema());
			longitude.put("position", jsonPosition.get("longitude").getAsJsonObject().get("position").getAsDouble());
			longitude.put("sense", jsonPosition.get("longitude").getAsJsonObject().get("sense").getAsString());
			position.put("longitude", longitude);
			
			//Quality optional
			GenericRecord quality = new GenericData.Record(this.schema.getField("position").schema().getField("quality").schema());
			if (jsonPosition.has("quality")) {
				JsonObject jsonQuality = jsonPosition.get("quality").getAsJsonObject();
				quality.put("hdop", jsonQuality.get("hdop").getAsDouble());
				quality.put("sats", jsonQuality.get("sats").getAsInt());
				quality.put("type", jsonQuality.get("type").getAsInt());
			}
			position.put("quality", quality);
		
		gpsRecord.put("position", position);
		
		//sac optional
		if (jsonData.has("sace")) {
			gpsRecord.put("sac", jsonData.get("sac").getAsInt());
		}
		
		//sasssite
		gpsRecord.put("sassite", jsonData.get("sassite").getAsString());
		
		//satelliteid
		gpsRecord.put("satelliteid", jsonData.get("satelliteid").getAsString());
		
		//time
		GenericRecord time = new GenericData.Record(this.schema.getField("time").schema());
		JsonObject jsonTime = jsonData.get("time").getAsJsonObject();
		time.put("capture", jsonTime.get("capture").getAsLong());
		
		if (jsonTime.has("measure")) {
			time.put("measure", jsonTime.get("capture").getAsLong());
		}
		gpsRecord.put("time",time);		
		
		//updatereason optional
		if (jsonData.has("updatereason")) {
			gpsRecord.put("updatereason", jsonData.get("updatereason").getAsInt());
		}
	
		//Return a generic record filled with data from the JSON message.
		return gpsRecord;
	}
	
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void initialize() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Event intercept(Event arg0) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public List<Event> intercept(List<Event> events) {
		//If schema has not been set, set it by retrieving it from the first message. 
		//This is done only once since schema is the same for every message.
		if (this.schema == null) {
			//Get schema from the first(index 0) event.
			this.schema = getSchema(events.get(0));

			//If schema is still not set, there is an error in the event header.
			//Interceptor returns null (no data) and prints an error message.
			if (this.schema == null) {
				System.out.println("ERROR: Couldn't set schema.");
				return null;
			}
		}
		
		//Create a byteArrayOutputStream. Serialized avro data is written to this stream.
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		//DataFileWriter is used for combining a sequence of avro records attached with one schema. 
		//Avro records are appended to this writer.
		DataFileWriter<Object> datumWriter = new DataFileWriter<Object>(new GenericDatumWriter<Object>());
		
		try {
			//Create datumwriter based on avro schema. Output of this writer is written to ByteArrayOutputStream. 
			datumWriter.create(this.schema, out);
			
			//Events is a list with events. Every event is processed.
			for (Event event : events) {
				//First step is to extract the JSON message from the event
				JsonObject jsonData = getJSON(event);
				//Next step is to convert the JSON data to AVRO format.
				//The avrorecord which is returned will be appended to the datumwriter.
				//The datumwriter has the same schema.
				datumWriter.append(getAvroRecord(jsonData));
			}
			//Close the datum writer. Output is written to out(ByteArrayOutputStream).
			datumWriter.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//Get a copy of the first(index 0) event.
    	Event returnEvent = events.get(0);
    	//Clear The list with events.
    	//This is needed because we just want one event with all json data with one schema
    	events.clear();
    	//Set the the data from out as the body of the return event.
    	returnEvent.setBody(out.toByteArray());
    	//Add this even to the list with events.
    	events.add(returnEvent);
		
		return events;
	}
	
    public static class Builder implements Interceptor.Builder
    {
        @Override
        public void configure(Context context) {
            // TODO Auto-generated method stub
        }

        @Override
        public Interceptor build() {
            return new GPSRecordEvent();
        }

    }
	
}