// importing Libraries 
import java.io.IOException; 
import java.util.Iterator; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.conf.Configuration; 

public class MaxMin { 

	
	// Mapper 
	
	/*MaxTemperatureMapper class is static 
	* and extends Mapper abstract class 
	* having four Hadoop generics type 
	* LongWritable, Text, Text, Text. 
	*/
	
	
	public static class MaxTemperatureMapper extends
			Mapper<LongWritable, Text, Text, Text> { 
		
		/** 
		* @method map 
		* This method takes the input as a text data type. 
		* Now leaving the first five tokens, it takes 
		* 6th token is taken as temp_max and 
		* 7th token is taken as temp_min. Now 
		* temp_max > 30 and temp_min < 15 are 
		* passed to the reducer. 
		*/

	 
	public static final int MISSING = 9999; 
		
	@Override
		public void map(LongWritable arg0, Text Value, Context context) 
				throws IOException, InterruptedException { 

		// Convertit la ligne(Record) en
		// String et la stocke dans une
		// variable du nom de la ligne 
			
		String line = Value.toString(); 
			
			// Vérifie les lignes vides 
			if (!(line.length() == 0)) { 
				
				// Du caractère 6 à 14 nous avons 
				// la date complète dans le dataset 
				String date = line.substring(6, 14); 

				// Nous avons la temperature maximale
				// en prenant du 39eme caractère au 45ème.
				float temperature_Max = Float.parseFloat(line.substring(39, 45).trim()); 
				
				// La même chose que précédemment avec le minimum 
				// du 47 au 53
				
				float temperature_Min = Float.parseFloat(line.substring(47, 53).trim()); 

				// si la temperature est supérieure à 30°, 
				// il s'agit d'un jour chaud 
				if (temperature_Max > 30.0) { 
					
					// Jour chaud 
					context.write(new Text("Ce jour est un jour chaud :" + date), 
										new Text(String.valueOf(temperature_Max))); 
				} 

				// Si la temperature min est inférieure à 15 
				// c'est un jour froid 
				if (temperature_Min < 10) { 
					
					// Cold day 
					context.write(new Text("Ce jour est un jour froid :" + date), 
							new Text(String.valueOf(temperature_Min))); 
				} 
			} 
		} 

	} 

// Reducer 
	
	//Nous utilisons ici la méthode du Reducer
	
	public static class MaxTemperatureReducer extends
			Reducer<Text, Text, Text, Text> { 

		/** 
		* @method reduce 
		* This method takes the input as key and 
		* list of values pair from the mapper, 
		* it does aggregation based on keys and 
		* produces the final context. 
		*/
		
		public void reduce(Text Key, Iterator<Text> Values, Context context) 
				throws IOException, InterruptedException { 

			
			// Nous mettons toutes les valeurs dans 
			// une variable temperature de type String 
			String temperature = Values.next().toString(); 
			context.write(Key, new Text(temperature)); 
		} 

	} 



	
	
	public static void main(String[] args) throws Exception { 

		// On lit la configuration par défaut
		// du cluster des fichiers XML
		Configuration conf = new Configuration(); 
		
		// On initialise le job avec 
		// configuration par defaut du cluster	 
		@SuppressWarnings("depreciation")
		Job job = Job.getInstance(conf, "CalculMeteo");
		 
		
		// On assigne un nom de class 
		job.setJarByClass(MaxMin.class); 
		
		// On definit le nom de la class du mapper 
				job.setMapperClass(MaxTemperatureMapper.class); 
				
				// On definit le nom de la classe du reducer
				job.setReducerClass(MaxTemperatureReducer.class); 

		// Les clés sortants du mappeurs
		job.setMapOutputKeyClass(Text.class); 
		
		// Les valeurs sortants du mappeur 
		job.setMapOutputValueClass(Text.class); 

		

		// On definit le format entrant qui est responsable 
		// de l'analyse du dataset
		// en une paire de clé valeur
		job.setInputFormatClass(TextInputFormat.class); 
		
		// On definit le format sortant qui est responsable 
		// de l'analyse du dataset
		// en une paire de clé valeur
		job.setOutputFormatClass(TextOutputFormat.class); 

		// On envoie le 2eme argument
		// comme chemin dans la variable Path
		Path OutputPath = new Path(args[1]); 

		// On configure le chemin entrant
		// depuis le filesystem dans le job
		FileInputFormat.addInputPath(job, new Path(args[0])); 

		// On configure le chemin sortant
		// depuis le filesystem dans le job
		FileOutputFormat.setOutputPath(job, new Path(args[1])); 

		// On supprime le context path  
		// depuis hdfs automatiquement pour ne pas le supprimer 
		// de manière explicite
		OutputPath.getFileSystem(conf).delete(OutputPath); 

		// On execute le job seulement si la valeur est fausse.
		System.exit(job.waitForCompletion(true) ? 0 : 1); 

	} 
} 
