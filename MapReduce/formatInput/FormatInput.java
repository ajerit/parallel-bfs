import java.io.IOException;
import java.util.StringTokenizer;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// EJECUTAR: hadoop jar list.jar BFS InputPath OutputFase1 NODOINICIAL
// Indicar en NODOINICIAL el ID del usuario que será el node de inicio

public class FormatInput {

  static private String nodoInicial;

  public static class InputMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text user = new Text();
    private Text follower = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer tokens = new StringTokenizer(value.toString());
      
      // Leer user y su follower
      user.set(tokens.nextToken());
      follower.set(tokens.nextToken());
      context.write(user, follower);
    }
  }

  public static class InputReducer
       extends Reducer<Text,Text,Text,Text> {

    private Text followerList = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      StringBuilder followersLine = new StringBuilder();

      // Añadir distancia maxima y color
      if (nodoInicial.equals(key.toString())) {
        followersLine.append("0|GRIS|");
      } else {
        Integer infinito = Integer.MAX_VALUE;
        followersLine.append(infinito.toString());
        followersLine.append("|BLANCO|");
      }

      // Juntar followers en un arreglo
      List<String> arregloFol = new ArrayList<>();
      for(Text f : values){
        arregloFol.add(f.toString());
      }

      // Concatenar followers con "," como separador
      String listaTemp = String.join(",", arregloFol);
      followersLine.append(listaTemp);
      followerList.set(followersLine.toString());

      context.write(key, followerList);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "FormatInput");
    job.setJarByClass(FormatInput.class);
    job.setMapperClass(InputMapper.class);
    job.setReducerClass(InputReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path("/input-bfs"));
    // Obtener nodo inicial para inicializar datos
    nodoInicial = args[1];
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
