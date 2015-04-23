



resolvers += Resolver.mavenLocal


javaOptions in run += "-Xmx8G"


scalacOptions += "-Yresolve-term-conflict:package" 


fork in run := true


libraryDependencies += "org.apache.thrift" % "libfb303" % "0.6.1"