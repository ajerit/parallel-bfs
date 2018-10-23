import java.io.IOException;
import java.util.StringTokenizer;
import java.util.List;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BFS {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text user = new Text();
    private IntWritable distancia = new IntWritable();
    private Text nodo = new Text();
    private Text distFollower = new Text();
    private Text follower = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      // Recuperar datos del Nodo
      // parts[0] = "id"
      // parts[1] = "dist|color|f1,f2,f3"
      String temp = value.toString();
      String[] parts = temp.split("\t", 2);

      // infoNodo[0] = "dist"
      // infoNodo[1] = "color"
      // infoNodo[2] = "f1,f2,f3"
      String[] infoNodo = parts[1].split("\\|", 3);
      distancia.set(Integer.parseInt(infoNodo[0]));

      String color = infoNodo[1];
      boolean colorearNegro = false;

      // Revisar y devolver followers con distancia si el nodo es GRIS
      if (color.equals("GRIS")) {
        StringTokenizer followers = new StringTokenizer(infoNodo[2], ",");
        Integer d = 0;
        while (followers.hasMoreTokens()) {
          if (distancia.get() == Integer.MAX_VALUE) {
            d = Integer.MAX_VALUE;
          } else {
            d = 1 + distancia.get();
          }
          // Colorear adyacencias de GRIS
          StringBuilder infoFollow = new StringBuilder();
          infoFollow.append(d.toString());
          infoFollow.append("|GRIS");
          distFollower.set(infoFollow.toString());
          follower.set(followers.nextToken());

          colorearNegro = true;
          
          context.write(follower, distFollower);
        }
      }

      // Reconstruir info del nodo y cambiar color a NEGRO
      StringBuilder lineaTemp = new StringBuilder();
      // Distancia, color y followers
      lineaTemp.append(infoNodo[0]);
      if (colorearNegro) {
        lineaTemp.append("|NEGRO|");
      } else {
        lineaTemp.append("|"+ color +"|");
      }
      lineaTemp.append(infoNodo[2]);
      
      user.set(parts[0]);
      nodo.set(lineaTemp.toString());
      // Devolver linea del nodo
      context.write(user, nodo);
    }
  }

  public static class BFSReducer
       extends Reducer<Text,Text,Text, Text> {

        private Text followerList = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      // VALUES contiene una lista de distancias y un nodo
      Integer distMin = Integer.MAX_VALUE;
      Integer distActual = Integer.MAX_VALUE;
      String colorActual;
      String colorFinal = "BLANCO";

      // infoNodo[0] = "dist"
      // infoNodo[1] = "color"
      // infoNodo[2] = "f1,f2,f3"
      String[] infoNodo = new String[3];

      for(Text val : values){
        String tempVal = val.toString();
        if (StringUtils.countMatches(tempVal, "|") > 1) {
          infoNodo = tempVal.split("\\|", 3);
          distActual = Integer.parseInt(infoNodo[0]);
          colorActual = infoNodo[1];
        } else {
          String[] distYColor = tempVal.split("\\|", 2); 
          Integer d = Integer.parseInt(distYColor[0]);
          colorActual = distYColor[1];

          if (d < distMin) {
            // Actualizar distMin
            distMin = d;
          }
        }
        // Caso borde del nodo inicio
        if (distMin < distActual ) {
          distActual = distMin;
        }

        if (colorActual.equals("NEGRO")) {
          colorFinal = "NEGRO";
        } else if (colorActual.equals("GRIS") && colorFinal.equals("BLANCO")) {
          colorFinal = "GRIS";
        } else if (colorActual.equals("BLANCO") && colorFinal.equals("BLANCO")) {
          colorFinal = "BLANCO";
        } else {
          colorFinal = "GRIS";
        }
      }

      // Reconstruir linea del nodo
      StringBuilder followersLine = new StringBuilder();

      // Distancia nueva
      followersLine.append(distActual.toString());
      followersLine.append("|");
      // Color nuevo
      followersLine.append(colorFinal);
      followersLine.append("|");
      
      // Lista de followers
      /*for (String x : infoNodo) {
        System.out.println(x);
      }*/

      if (infoNodo.length > 2) {
        followersLine.append(infoNodo[2]);
      } else {
        followersLine.append("");
      }
      followerList.set(followersLine.toString());

      context.write(key, followerList);
    }
  }

  public static void main(String[] args) throws Exception {
    
    boolean finished = false;
    int res = 0;
    long iteracion = 0;
    Configuration confFS = new Configuration();
    confFS.addResource(new Path("/usr/hadoop/etc/core-site.xml"));
    confFS.addResource(new Path("/usr/hadoop/etc/hdfs-site.xml"));
    FileSystem hFS = FileSystem.get(confFS);

    while (!finished) {
      // Iteraciones
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "BFS");
      job.setJarByClass(BFS.class);
      job.setMapperClass(TokenizerMapper.class);
      job.setReducerClass(BFSReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      String input;
      if (iteracion == 0) {
        input = "/input-bfs";
      } else {
        input = "/output-bfs-" + iteracion;
      }

      String output = "/output-bfs-" + (iteracion + 1);

      FileInputFormat.addInputPath(job, new Path(input));
      FileOutputFormat.setOutputPath(job, new Path(output));
      res = job.waitForCompletion(true) ? 0 : 1;

      iteracion++;

      // Revisar condición de parada
      if (iteracion >= 2) {
        FileChecksum hash1 = hFS.getFileChecksum(new Path("hdfs://localhost:9000/output-bfs-" + iteracion + "/part-r-00000"));
        FileChecksum hash2 = hFS.getFileChecksum(new Path("hdfs://localhost:9000/output-bfs-" + (iteracion - 1) + "/part-r-00000"));
        System.out.println("Iteraciones totales => " + iteracion);
       
        if (hash1.equals(hash2)) {
          finished = true;
        }
      }
    }

    System.exit(res);
  }
}
