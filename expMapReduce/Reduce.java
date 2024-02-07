package bigdata.expMapReduce;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public  class Reduce extends Reducer<Text,Text,Text,Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
	throws IOException, InterruptedException {
		try {
			// ###DEBUT REDUCE: (key, values) -> (key, res)
			int res = 0;
			int cnt = 0;
			// - parcourir les valeurs du groupe
			for (Text value : values) {
				res += Integer.parseInt(value.toString());
				cnt++;
			}
			res /= cnt;
			// ###FIN

			// retourner le résultat
			context.write(key, new Text(res.toString()));

		} catch(JSONException e) {
			e.printStackTrace();
		}
	}
}
