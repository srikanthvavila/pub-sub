import abc
import six
import yaml
import os
import logging

class PipelineException(Exception):
    def __init__(self, message, pipeline_cfg):
        self.msg = message
        self.pipeline_cfg = pipeline_cfg

    def __str__(self):
        return 'Pipeline %s: %s' % (self.pipeline_cfg, self.msg)


class Source(object):
    """Represents a source of samples or events."""

    def __init__(self, cfg):
        self.cfg = cfg

        try:
            self.name = cfg['name']
            self.sinks = cfg.get('sinks')
        except KeyError as err:
            raise PipelineException(
                "Required field %s not specified" % err.args[0], cfg)

    def __str__(self):
        return self.name

    def check_sinks(self, sinks):
        if not self.sinks:
            raise PipelineException(
                "No sink defined in source %s" % self,
                self.cfg)
        for sink in self.sinks:
            if sink not in sinks:
                raise PipelineException(
                    "Dangling sink %s from source %s" % (sink, self),
                    self.cfg)
    def check_source_filtering(self, data, d_type):
        """Source data rules checking

        - At least one meaningful datapoint exist
        - Included type and excluded type can't co-exist on the same pipeline
        - Included type meter and wildcard can't co-exist at same pipeline
        """
        if not data:
            raise PipelineException('No %s specified' % d_type, self.cfg)

        if ([x for x in data if x[0] not in '!*'] and
           [x for x in data if x[0] == '!']):
            raise PipelineException(
                'Both included and excluded %s specified' % d_type,
                self.cfg)

        if '*' in data and [x for x in data if x[0] not in '!*']:
            raise PipelineException(
                'Included %s specified with wildcard' % d_type,
                self.cfg)

    @staticmethod
    def is_supported(dataset, data_name):
        # Support wildcard like storage.* and !disk.*
        # Start with negation, we consider that the order is deny, allow
        if any(fnmatch.fnmatch(data_name, datapoint[1:])
               for datapoint in dataset if datapoint[0] == '!'):
            return False

        if any(fnmatch.fnmatch(data_name, datapoint)
               for datapoint in dataset if datapoint[0] != '!'):
            return True

        # if we only have negation, we suppose the default is allow
        return all(datapoint.startswith('!') for datapoint in dataset)


@six.add_metaclass(abc.ABCMeta)
class Pipeline(object):
    """Represents a coupling between a sink and a corresponding source."""

    def __init__(self, source, sink):
        self.source = source
        self.sink = sink
        self.name = str(self)

    def __str__(self):
        return (self.source.name if self.source.name == self.sink.name
                else '%s:%s' % (self.source.name, self.sink.name))


class SamplePipeline(Pipeline):
    """Represents a pipeline for Samples."""

    def get_interval(self):
        return self.source.interval

class SampleSource(Source):
    """Represents a source of samples.

    In effect it is a set of pollsters and/or notification handlers emitting
    samples for a set of matching meters. Each source encapsulates meter name
    matching, polling interval determination, optional resource enumeration or
    discovery, and mapping to one or more sinks for publication.
    """

    def __init__(self, cfg):
        super(SampleSource, self).__init__(cfg)
        try:
            try:
                self.interval = int(cfg['interval'])
            except ValueError:
                raise PipelineException("Invalid interval value", cfg)
            # Support 'counters' for backward compatibility
            self.meters = cfg.get('meters', cfg.get('counters'))
        except KeyError as err:
            raise PipelineException(
                "Required field %s not specified" % err.args[0], cfg)
        if self.interval <= 0:
            raise PipelineException("Interval value should > 0", cfg)

        self.resources = cfg.get('resources') or []
        if not isinstance(self.resources, list):
            raise PipelineException("Resources should be a list", cfg)

        self.discovery = cfg.get('discovery') or []
        if not isinstance(self.discovery, list):
            raise PipelineException("Discovery should be a list", cfg)
        self.check_source_filtering(self.meters, 'meters')

    def support_meter(self, meter_name):
        return self.is_supported(self.meters, meter_name)


class Sink(object):

    def __init__(self, cfg, transformer_manager):
        self.cfg = cfg

        try:
            self.name = cfg['name']
            # It's legal to have no transformer specified
            self.transformer_cfg = cfg.get('transformers') or []
        except KeyError as err:
            raise PipelineException(
                "Required field %s not specified" % err.args[0], cfg)

        if not cfg.get('publishers'):
            raise PipelineException("No publisher specified", cfg)

      

class SampleSink(Sink):
    def Testfun(self):
        pass
      

SAMPLE_TYPE = {'pipeline': SamplePipeline,
               'source': SampleSource,
               'sink': SampleSink}


class PipelineManager(object):

     def __init__(self, cfg, transformer_manager, p_type=SAMPLE_TYPE):
        self.pipelines = []
        if 'sources' in cfg or 'sinks' in cfg:
            if not ('sources' in cfg and 'sinks' in cfg):
                raise PipelineException("Both sources & sinks are required",
                                        cfg)
            #LOG.info(_('detected decoupled pipeline config format'))
            logging.info("detected decoupled pipeline config format %s",cfg)
            sources = [p_type['source'](s) for s in cfg.get('sources', [])]
            sinks = {}
            for s in cfg.get('sinks', []):
                if s['name'] in sinks:
                    raise PipelineException("Duplicated sink names: %s" %
                                            s['name'], self)
                else:
                    sinks[s['name']] = p_type['sink'](s, transformer_manager)
            for source in sources:
                source.check_sinks(sinks)
                for target in source.sinks:
                    pipe = p_type['pipeline'](source, sinks[target])
                    if pipe.name in [p.name for p in self.pipelines]:
                        raise PipelineException(
                            "Duplicate pipeline name: %s. Ensure pipeline"
                            " names are unique. (name is the source and sink"
                            " names combined)" % pipe.name, cfg)
                    else:
                        self.pipelines.append(pipe)
        else:
            #LOG.warning(_('detected deprecated pipeline config format'))
            logging.warning("detected deprecated pipeline config format")
            for pipedef in cfg:
                source = p_type['source'](pipedef)
                sink = p_type['sink'](pipedef, transformer_manager)
                pipe = p_type['pipeline'](source, sink)
                if pipe.name in [p.name for p in self.pipelines]:
                    raise PipelineException(
                        "Duplicate pipeline name: %s. Ensure pipeline"
                        " names are unique" % pipe.name, cfg)
                else:
                    self.pipelines.append(pipe)
     

def _setup_pipeline_manager(cfg_file, transformer_manager, p_type=SAMPLE_TYPE):
    if not os.path.exists(cfg_file):
        #cfg_file = cfg.CONF.find_file(cfg_file)
        print "File doesn't exists"   
        return False

    ##LOG.debug(_("Pipeline config file: %s"), cfg_file)
    logging.debug("Pipeline config file: %s", cfg_file)

    with open(cfg_file) as fap:
        data = fap.read()

    pipeline_cfg = yaml.safe_load(data)
     
    ##LOG.info(_("Pipeline config: %s"), pipeline_cfg)
    logging.info("Pipeline config: %s", pipeline_cfg)
    logging.debug("Pipeline config: %s", pipeline_cfg)
      
    return PipelineManager(pipeline_cfg,
                           None, p_type)
    

