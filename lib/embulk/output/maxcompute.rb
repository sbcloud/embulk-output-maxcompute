Embulk::JavaPlugin.register_output(
  "maxcompute", "org.embulk.output.maxcompute.MaxcomputeOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
