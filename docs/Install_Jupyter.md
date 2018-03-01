[< Table of Contents](README.md) | [Use ArchiveSpark with Jupyter >](Use_Jupyter.md)
:---|---:

# Install ArchiveSpark with Jupyter

In order to get you started more easily, we provide a pre-packaged and pre-configured [Docker](https://www.docker.com/) container with ArchiveSpark and Jupyter ready to run, just one command away: https://github.com/helgeho/ArchiveSpark-docker

## Manual Installation

The following steps explain how to install ArchiveSpark on your cluster or local environemnt:

1. Since ArchiveSpark is based on Apache Spark, as a first step you need to download and unpack Spark.
It can be downloaded from here: https://spark.apache.org/downloads.html.
Please unpack it and remember the location as you will need it in the following.

2. Next, you need to install [Jupyter](http://jupyter.org).
Please follow the official instruction to do so:
https://jupyter.readthedocs.io/en/latest/install.html

3. In order to use ArchiveSpark with Jupyter, a Scala kernel is required that lets you run ArchiveSpark instructions interactively in a Jupyter notebook.
We recommend the use of [Apache Toree](https://toree.apache.org/) for this.
As the setup of Toree is not very straight-forward, we provide a pre-configured package under:
http://www.l3s.de/~holzmann/toree2_dynamic_lib.tar.gz.

4. Now, you will need to create a libraries folder in which you store ArchiveSpark as well as all required dependencies.
Create this directory in any path of your choice and copy the JAR files required by ArchiveSpark to it.
The latest main JAR can be downloaded from [Maven Central](https://search.maven.org/#search%7Cga%7C1%7Carchivespark) or the released version on GitHub: https://github.com/helgeho/ArchiveSpark/releases
In addition to that, we also provide a pre-packaged JAR that includes all required dependencies there.

5. Finally, you will need to configure the Jupyter kernel by creating a *kernel spec*.
This has to be stored in Jupyter's `kernels` directory, which is located in its data path.
Under Linux, this path by default is `~/.local/share/jupyter/kernels`.
To find the path on your system, you can run the `jupyter --paths`.
More details can be found here: http://jupyter.readthedocs.io/en/latest/projects/jupyter-directories.html
Now create a directory inside the `kernels` directory, and name it, e.g., `ArchiveSpark`.
Inside this folder, create a `kernel.json` file with the following content:

```json
{
    "display_name": "ArchiveSpark",
    "language_info": { "name": "scala" },
    "argv": [
       	"/ABSOLUTE/PATH/TO/JUPYTER/bin/run.sh",
	"--profile",
        "{connection_file}"
     ],
     "codemirror_mode": "scala",
     "env": {
         "SPARK_OPTS": "--master=yarn --deploy-mode client --conf spark.default.parallelism=100 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.executor.userClassPathFirst=true",
         "CAPTURE_STANDARD_OUT": "true",
         "CAPTURE_STANDARD_ERR": "true",
         "SEND_EMPTY_OUTPUT": "false",
         "SPARK_HOME": "/ABSOLUTE/PATH/TO/SPARK",
         "HADOOP_CONF_DIR": "/etc/hadoop/conf",
         "LIB_PATH": "/ABSOLUTE/PATH/TO/LIBRARIES"
      }
}
```

Please replace the paths written in CAPITALS by your paths as created in the steps above.
Also, change the `HADOOP_CONF_DIR` property if required (it is set to the default on most Hadoop clusters).

This kernel spec shows the default configuration to run ArchiveSpark on a Hadoop/YARN cluster in Spark client mode.
If you want to run it locally on your own machine, remove the `HADOOP_CONF_DIR` property entirely, remove the `--deploy mode` option in `SPARK_OPTS` and change `--master` from `yarn` to `local[*]`.
Additional Spark options that can be specified in the `SPARK_OPTS` are listed on
https://spark.apache.org/docs/latest/configuration.html

*Please note: The following non-default parameters should always be used in ArchiveSpark to avoid serialization issues and dependency conflicts: `--conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.executor.userClassPathFirst=true`*

6. To run Jupter, go to a directory of your choice, where your notebooks will be placed, and run `jupyter notebook`.
More on the use of ArchiveSpark with Jupyter can be found under [Use ArchiveSpark with Jupyter](Use_Jupyter.md).

[< Table of Contents](README.md) | [Use ArchiveSpark with Jupyter >](Use_Jupyter.md)
:---|---: