OpenTsdbSkylinePublisher
========================

RTPublisher Plugin for OpenTSDB 2.0 to send metrics to skyline 


You might want to compile it with "mvn assembly:single" to have all dependencies in the fat jar...


Add these settings to your opentsdb.conf:

tsd.core.plugin_path = <your plugin directory>  
tsd.rtpublisher.enable = true  
tsd.rtpublisher.plugin = net.gutefrage.tsdb.SkylinePublisher  
tsd.plugin.skyline.port = <skyline UDP port, normally 2025>  
tsd.plugin.skyline.host = <your skyline host>  



