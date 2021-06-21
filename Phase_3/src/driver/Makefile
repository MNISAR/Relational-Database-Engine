JDKPATH = /usr
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

# test driver

AppDriverPhase3:AppDriverPhase3.java
	$(JAVAC) AppDriverPhase3.java

driver3: AppDriverPhase3
	$(JAVA) driver.AppDriverPhase3