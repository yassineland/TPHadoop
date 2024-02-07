package expMapReduce;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.json.*;

public class Map extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context)
	throws IOException, InterruptedException
	{
		try {
			// prendre une ligne (String) du fichier
			String line = value.toString();
			// convertir la ligne � un document JSON
			JSONObject doc = new JSONObject(line);

			// ###DEBUT MAP: doc -> write(key, value)
			// - cl�: doc.genre
			String mkey = doc.getString("genre");
			// - valeur: doc.actors.length
			int mvalue = doc.getJSONArray("actors").length();
			// - write(cl�, valeur)
			context.write(new Text(mkey.toString()), new Text(String.valueOf(mvalue)));
			// ###FIN

		} catch(JSONException e) {
			e.printStackTrace();
		}
	}
}
