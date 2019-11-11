# WI Project
The goal is to predict the click on an ads based on a dataset from the real world.

The program must take a json file of user data as input and produce a csv file enhanced by a first column named "label" that will contains the wether or not (true or false) the user would had clicked.


## Build the project

Download this project, then go to the directory and open a terminal in the source folder. Then execute the following command:
<code> sbt assembly </code>

A jar executable called <b>TheIllusionists-assembly-0.1.jar </b> should be created in the source folder at <code>target/scala-2.12</code>.

You can move the executable from his place to the source folder by executing the following command:
<code> mv target/scala-2.12/TheIllusionists-assembly-0.1.jar TheIllusionists.jar </code>

## Run the project

:warning: <b>Before launching the following command be sure the folder "models/LR" exists and is not empty</b> :warning:

Once you moved the executable to the source folder, you can run the project by using:
<code> java -jar TheIllusionists.jar *"path to the json data file"*</code>

A CSV file called <code>part-0000-xxxx.csv</code> should be created in the <code>result</code> folder with the prediction of the model in the first column <code>label</code>.
