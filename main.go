package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
	"gopkg.in/urfave/cli.v1"
)

type PageResponse struct {
	Items []struct {
		Key   string `json:"key"`
		Label []struct {
			Text string `json:"text"`
		} `json:"label"`
		Source string `json:"source"`
	} `json:"items"`
	Layout string `json:"layout"`
}

func main() {
	app := cli.NewApp()
	app.Name = "genecache"
	app.Usage = "cli for caching all dictybase genes"
	app.Version = "1.0.0"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "log-level",
			Usage: "log level for the application",
			Value: "info",
		},
		cli.StringFlag{
			Name:  "log-format",
			Usage: "format of the logging out, either of json or text",
			Value: "json",
		},
		cli.StringFlag{
			Name:  "input,i",
			Usage: "Input file with list of paired dictybase gene and transcript ids[required]",
		},
		cli.StringFlag{
			Name:  "log-file",
			Usage: "Name of the output log file, default goes to STDERR",
		},
		cli.StringFlag{
			Name:  "url,u",
			Usage: "Base url for dictybase",
			Value: "http://dictybase.org",
		},
	}
	app.Action = cacheGeneAction
	app.Before = validateCacheAction
	app.Run(os.Args)
}

func cacheGeneAction(c *cli.Context) error {
	log, err := getLogger(c)
	if err != nil {
		return cli.NewExitError(err.Error(), 2)
	}
	r, err := os.Open(c.GlobalString("input"))
	if err != nil {
		return cli.NewExitError(
			fmt.Sprintf("cannot open file %s %s", c.GlobalString("input"), err.Error()),
			2,
		)
	}
	defer r.Close()
	base := c.GlobalString("url")
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		idSlice := strings.Split(scanner.Text(), "\t")
		geneId := idSlice[0]
		transId := idSlice[1]
		urls := []string{
			fmt.Sprintf("%s/%s/%s/%s.json", base, "gene", geneId, "gene"),
			fmt.Sprintf("%s/%s/%s/%s/%s.json", base, "gene", geneId, "protein", transId),
		}
		for _, u := range urls {
			resp, err := http.Get(u)
			if err != nil {
				log.WithFields(logrus.Fields{
					"id":   geneId,
					"url":  u,
					"kind": "fetch",
				}).Error(err.Error())
				continue
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				log.WithFields(logrus.Fields{
					"id":         geneId,
					"url":        u,
					"kind":       "http",
					"statusCode": resp.StatusCode,
				}).Error("http error in fetching url")
				continue
			}
			log.WithFields(logrus.Fields{
				"id":   geneId,
				"url":  u,
				"kind": "caching",
			}).Debug("success in fetching and caching url")
			// Now get all panel urls and cache them
			presp := make([]PageResponse, 0)
			if err := json.NewDecoder(resp.Body).Decode(&presp); err != nil {
				log.WithFields(logrus.Fields{
					"id":   geneId,
					"url":  u,
					"kind": "decoding",
				}).Error(err.Error())
				continue
			}
			for _, item := range presp[0].Items {
				purl := fmt.Sprintf("%s%s", base, item.Source)
				resp, err := http.Get(purl)
				if err != nil {
					log.WithFields(logrus.Fields{
						"id":   geneId,
						"url":  purl,
						"kind": "fetch",
					}).Error(err.Error())
					continue
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					log.WithFields(logrus.Fields{
						"id":         geneId,
						"url":        purl,
						"kind":       "http",
						"statusCode": resp.StatusCode,
					}).Error("http error in fetching url")
					continue
				}
				log.WithFields(logrus.Fields{
					"id":   geneId,
					"url":  purl,
					"kind": "caching",
				}).Debug("success in fetching and caching url")
			}
		}
		// reference url
		rurl := fmt.Sprintf("%s/%s/%s/%s.json", base, "gene", geneId, "references")
		resp, err := http.Get(rurl)
		if err != nil {
			log.WithFields(logrus.Fields{
				"id":   geneId,
				"url":  rurl,
				"kind": "fetch",
			}).Error(err.Error())
			return cli.NewExitError(
				fmt.Sprintf("error fetching url %s %s", rurl, err.Error()),
				2,
			)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			log.WithFields(logrus.Fields{
				"id":         geneId,
				"url":        rurl,
				"kind":       "http",
				"statusCode": resp.StatusCode,
			}).Error("http error in fetching url")
			return cli.NewExitError(
				fmt.Sprintf("http error %d in fetching url %s %s", resp.StatusCode, rurl, err.Error()),
				2,
			)
		}
		log.WithFields(logrus.Fields{
			"id":   geneId,
			"url":  rurl,
			"kind": "caching",
		}).Debug("success in fetching and caching url")
		log.WithFields(logrus.Fields{
			"id":   geneId,
			"kind": "caching",
		}).Info("success in caching gene id")
	}
	if err := scanner.Err(); err != nil {
		return cli.NewExitError(err.Error(), 2)
	}
	return nil
}

func validateCacheAction(c *cli.Context) error {
	if !c.GlobalIsSet("input") {
		return cli.NewExitError("input file is not given", 2)
	}
	return nil
}

func getLogger(c *cli.Context) (*logrus.Logger, error) {
	log := logrus.New()
	switch c.GlobalString("log-format") {
	case "text":
		log.Formatter = &logrus.TextFormatter{
			TimestampFormat: "02/Jan/2006:15:04:05",
			FullTimestamp:   true,
		}
	default:
		log.Formatter = &logrus.JSONFormatter{
			TimestampFormat: "02/Jan/2006:15:04:05",
		}
	}
	if c.GlobalIsSet("log-file") {
		w, err := os.Create(c.GlobalString("log-file"))
		if err != nil {
			return log, fmt.Errorf("unable to open log file %s", err)
		}
		log.Out = w
	} else {
		log.Out = os.Stderr
	}
	l := c.GlobalString("log-level")
	switch l {
	case "debug":
		log.Level = logrus.DebugLevel
	case "warn":
		log.Level = logrus.WarnLevel
	case "error":
		log.Level = logrus.ErrorLevel
	case "fatal":
		log.Level = logrus.FatalLevel
	case "panic":
		log.Level = logrus.PanicLevel
	}
	return log, nil
}
