package sql_exporter

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"

	"github.com/burningalchemist/sql_exporter/config"
	"github.com/danieljoos/wincred"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
)

const envDsnOverride = "SQLEXPORTER_TARGET_DSN"

var dsnOverride = flag.String("config.data-source-name", "", "Data source name to override the value in the configuration file with.")

//assemble connection string for target to feed into existing driver method
func assemble_connectionstring(d DataSource) string {
	switch d.DBType {
	case "sqlserver":
		fmt.Println("Datasource is of type: sqlserver")
		if d.Param != "" {
			conString := "sqlserver://" + d.User + ":" + d.Password + "@" + d.Host + ":" + fmt.Sprint(d.Port) + "/" + d.Instance + "?param=" + d.Param
			return conString
		} else {
			conString := "sqlserver://" + d.User + ":" + d.Password + "@" + d.Host + ":" + fmt.Sprint(d.Port) + "/" + d.Instance
			return conString
		}
	case "mysql":
		fmt.Println("Datasource is of type: mysql")
		if d.Param != "" {
			conString := "mysql://" + d.User + ":" + d.Password + "@" + d.Host + ":" + fmt.Sprint(d.Port) + "/" + d.DBName + "?param=" + d.Param
			return conString
		} else {
			conString := "mysql://" + d.User + ":" + d.Password + "@" + d.Host + ":" + fmt.Sprint(d.Port) + "/" + d.DBName
			return conString
		}
	case "postgresql_libpq":
		fmt.Println("Datasource is of type: postgresql_libpq")
		if d.Param != "" {
			conString := "postgres://" + d.User + ":" + d.Password + "@" + d.Host + ":" + fmt.Sprint(d.Port) + "/" + d.DBName + "?param=" + d.Param
			return conString
		} else {
			conString := "postgres://" + d.User + ":" + d.Password + "@" + d.Host + ":" + fmt.Sprint(d.Port) + "/" + d.DBName
			return conString
		}
	case "postgresql_pgx":
		fmt.Println("Datasource is of type: postgresql_libpq")
		if d.Param != "" {
			conString := "pgx://" + d.User + ":" + d.Password + "@" + d.Host + ":" + fmt.Sprint(d.Port) + "/" + d.DBName + "?param=" + d.Param
			return conString
		} else {
			conString := "pgx://" + d.User + ":" + d.Password + "@" + d.Host + ":" + fmt.Sprint(d.Port) + "/" + d.DBName
			return conString
		}
	case "clickhouse":
		fmt.Println("Datasource is of type: clickhouse")
		if d.Param != "" {
			conString := "clickhouse://" + d.Host + ":" + fmt.Sprint(d.Port) + "?username=" + d.User + "&password=" + d.Password + "&database=" + d.DBName + "&param=" + d.Param
			return conString
		} else {
			conString := "clickhouse://" + d.Host + ":" + fmt.Sprint(d.Port) + "?username=" + d.User + "&password=" + d.Password + "&database=" + d.DBName
			return conString
		}
	case "snowflake":
		fmt.Println("Datasource is of type: snowflake")
		if d.Param != "" {
			conString := "snowflake://" + d.User + ":" + d.Password + "@" + d.Account + "/" + d.DBName + "?role=" + d.Role + "&warehouse=" + d.Warehouse + "&param=" + d.Param
			return conString
		} else {
			conString := "snowflake://" + d.User + ":" + d.Password + "@" + d.Account + "/" + d.DBName + "?role=" + d.Role + "&warehouse=" + d.Warehouse
			return conString
		}
	case "vertica":
		fmt.Println("Datasource is of type: vertica")
		if d.Param != "" {
			conString := "vertica://" + d.User + ":" + d.Password + "@" + d.Host + ":" + fmt.Sprint(d.Port) + "/" + d.DBName + "?param=" + d.Param
			return conString
		} else {
			conString := "vertica://" + d.User + ":" + d.Password + "@" + d.Host + ":" + fmt.Sprint(d.Port) + "/" + d.DBName
			return conString
		}
	}
	return ""
}

//Windows credential store interaction
type credential struct {
	username string
	password []byte
}

// Exporter is a prometheus.Gatherer that gathers SQL metrics from targets and merges them with the default registry.
type Exporter interface {
	prometheus.Gatherer

	// WithContext returns a (single use) copy of the Exporter, which will use the provided context for Gather() calls.
	WithContext(context.Context) Exporter
	// Config returns the Exporter's underlying Config object.
	Config() *config.Config
	UpdateTarget([]Target)
}

type exporter struct {
	config  *config.Config
	targets []Target

	ctx context.Context
}

// NewExporter returns a new Exporter with the provided config.
func NewExporter(configFile string) (Exporter, error) {
	c, err := config.Load(configFile)
	if err != nil {
		return nil, err
	}

	if val, ok := os.LookupEnv(envDsnOverride); ok {
		*dsnOverride = val
	}
	// Override the DSN if requested (and in single target mode).
	if *dsnOverride != "" {
		if len(c.Jobs) > 0 {
			return nil, fmt.Errorf("the config.data-source-name flag (value %q) only applies in single target mode", *dsnOverride)
		}
		c.Target.DSN = config.Secret(*dsnOverride)
	}

	// Generate DSN from data source fields.
	if c.Target.DataSource.MachineStore == true {
		cred, err := wincred.GetGenericCredential(c.Target.DataSource.CredName)
		if err != nil {
			return nil, fmt.Errorf("The credential (value %q) cannot be accessed", c.Target.DataSource.CredName)
		}
		c.Target.DataSource.User = string(cred.UserName)
		c.Target.DataSource.Password = string(cred.CredentialBlob)
		conString := assemble_connectionstring(c.Target.DataSource)
		c.Target.DSN = config.Secret(conString)
	} else {
		if c.Target.DataSource.User != "" {
			if c.Target.DataSource.Password != "" {
				conString := assemble_connectionstring(c.Target.DataSource)
				c.Target.DSN = config.Secret(conString)

			} else {
				return nil, fmt.Errorf("The password cannot be empty when use_credential_store is set to false")
			}
		} else {
			return nil, fmt.Errorf("The user field cannot be empty when use_credential_store is set to false")
		}
	}

	var targets []Target
	if c.Target != nil {
		target, err := NewTarget("", "", string(c.Target.DSN), c.Target.Collectors(), nil, c.Globals)
		if err != nil {
			return nil, err
		}
		targets = []Target{target}
	} else {
		if len(c.Jobs) > (config.MaxInt32 / 3) {
			return nil, errors.New("'jobs' list is too large")
		}
		targets = make([]Target, 0, len(c.Jobs)*3)
		for _, jc := range c.Jobs {
			job, err := NewJob(jc, c.Globals)
			if err != nil {
				return nil, err
			}
			targets = append(targets, job.Targets()...)
		}
	}

	return &exporter{
		config:  c,
		targets: targets,
		ctx:     context.Background(),
	}, nil
}

func (e *exporter) WithContext(ctx context.Context) Exporter {
	return &exporter{
		config:  e.config,
		targets: e.targets,
		ctx:     ctx,
	}
}

// Gather implements prometheus.Gatherer.
func (e *exporter) Gather() ([]*dto.MetricFamily, error) {
	var (
		metricChan = make(chan Metric, capMetricChan)
		errs       prometheus.MultiError
	)

	var wg sync.WaitGroup
	wg.Add(len(e.targets))
	for _, t := range e.targets {
		go func(target Target) {
			defer wg.Done()
			target.Collect(e.ctx, metricChan)
		}(t)
	}

	// Wait for all collectors to complete, then close the channel.
	go func() {
		wg.Wait()
		close(metricChan)
	}()

	// Drain metricChan in case of premature return.
	defer func() {
		for range metricChan {
		}
	}()

	// Gather.
	dtoMetricFamilies := make(map[string]*dto.MetricFamily, 10)
	for metric := range metricChan {
		dtoMetric := &dto.Metric{}
		if err := metric.Write(dtoMetric); err != nil {
			errs = append(errs, err)
			continue
		}
		metricDesc := metric.Desc()
		dtoMetricFamily, ok := dtoMetricFamilies[metricDesc.Name()]
		if !ok {
			dtoMetricFamily = &dto.MetricFamily{}
			dtoMetricFamily.Name = proto.String(metricDesc.Name())
			dtoMetricFamily.Help = proto.String(metricDesc.Help())
			switch {
			case dtoMetric.Gauge != nil:
				dtoMetricFamily.Type = dto.MetricType_GAUGE.Enum()
			case dtoMetric.Counter != nil:
				dtoMetricFamily.Type = dto.MetricType_COUNTER.Enum()
			default:
				errs = append(errs, fmt.Errorf("don't know how to handle metric %v", dtoMetric))
				continue
			}
			dtoMetricFamilies[metricDesc.Name()] = dtoMetricFamily
		}
		dtoMetricFamily.Metric = append(dtoMetricFamily.Metric, dtoMetric)
	}

	// No need to sort metric families, prometheus.Gatherers will do that for us when merging.
	result := make([]*dto.MetricFamily, 0, len(dtoMetricFamilies))
	for _, mf := range dtoMetricFamilies {
		result = append(result, mf)
	}
	return result, errs
}

// Config implements Exporter.
func (e *exporter) Config() *config.Config {
	return e.config
}

func (e *exporter) UpdateTarget(target []Target) {
	e.targets = target
}
