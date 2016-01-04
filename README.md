# BSc_thesis
Server management scripts and some practical exercises (Hadoop Java programs) created in context of the thesis

Title of the thesis: Design of Exercises for the Lecture Distributed Information Systems

Main programs:

- Each of the following Hadoop-program contains one CoffeeXML.java (consists of job-configurations and main-method)
- The prefix number in each description below is linking to the corresponding chapter in the thesis document
- Exercise papers can be found in the end of the thesis document (appendix)

6.1 Writing M/R Job on XML Data (Exercise 5; Practice 4)
- eclipse_workspace/src/hadoop/stack/sortUser

6.2 M/R Mapper as SQL Selects (Exercise 5; Practice 5 a))
- eclipse_workspace/src/hadoop/stack/shortPost

6.3 Group By and Order By with Shuffle (Exercise 5; Practice 5 c))
- eclipse_workspace/src/hadoop/stack/scoreDistribution

6.4 Joining Two XML Files (Exercise 5; Practice 6)
- eclipse_workspace/src/hadoop/stack/popular

In colloquium presented another sql-like join-program

Select answer.Id, question.Body, answer.Body
from Post question inner join Post answer
on question.AcceptedAnswerId = answer.Id

Id_____Body__________________Body

"42" | "How's the weather?" | "Great!"

- eclipse_workspace/src/hadoop/stack/demo