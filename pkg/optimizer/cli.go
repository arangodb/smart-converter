//
// DISCLAIMER
//
// Copyright 2016-2021 ArangoDB GmbH, Cologne, Germany
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Copyright holder is ArangoDB GmbH, Cologne, Germany
//

package optimizer

import (
	"os"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
)

func CLI() *cobra.Command {
	var c cli

	return c.Command()
}

type cli struct {
	threads int
	batch   int

	logLevel string

	extractVertexesArgs struct {
		In, Out string
	}

	extractEdgesArgs struct {
		In, Out string
	}

	mapEdgesArgs struct {
		Vertexes, Edges, TempA, TempB, Out string
	}

	optimizeArgs struct {
		In, Out string
	}

	translateArgs struct {
		Map, Vertexes, Edges, EdgesOut, EdgesMap, VertexesOut string
	}
}

func (c *cli) Command() *cobra.Command {
	cmd := &cobra.Command{}

	extractEdges := &cobra.Command{
		Use:     "extract-edges",
		RunE:    c.extractEdges,
		PreRunE: c.extractEdgesPre,
	}

	extractVertexes := &cobra.Command{
		Use:     "extract-vertexes",
		RunE:    c.extractVertexes,
		PreRunE: c.extractVertexesPre,
	}

	mapEdges := &cobra.Command{
		Use:     "map-edges",
		RunE:    c.mapEdges,
		PreRunE: c.mapEdgesPre,
	}

	optimize := &cobra.Command{
		Use:     "optimize",
		RunE:    c.optimize,
		PreRunE: c.optimizePre,
	}

	translate := &cobra.Command{
		Use:     "translate",
		RunE:    c.translate,
		PreRunE: c.translatePre,
	}

	f := extractVertexes.Flags()

	f.StringVar(&c.extractVertexesArgs.In, "in", "", "File with input documents")
	f.StringVar(&c.extractVertexesArgs.Out, "out", "", "File with output vertexes")

	f = extractEdges.Flags()

	f.StringVar(&c.extractEdgesArgs.In, "in", "", "File with input edges")
	f.StringVar(&c.extractEdgesArgs.Out, "out", "", "File with output edges vertexes")

	f = mapEdges.Flags()

	f.StringVar(&c.mapEdgesArgs.Vertexes, "vertexes", "", "File with mapped vertexes")
	f.StringVar(&c.mapEdgesArgs.Edges, "edges", "", "File with mapped edges vertexes")
	f.StringVar(&c.mapEdgesArgs.TempA, "temp.a", "", "Temporary file")
	f.StringVar(&c.mapEdgesArgs.TempB, "temp.b", "", "Temporary file")
	f.StringVar(&c.mapEdgesArgs.Out, "out", "", "Output file")

	f = optimize.Flags()

	f.StringVar(&c.optimizeArgs.In, "in", "", "File with input edges")
	f.StringVar(&c.optimizeArgs.Out, "out", "", "File with output edges vertexes")

	f = translate.Flags()

	f.StringVar(&c.translateArgs.Map, "map", "", "File with optimized map")
	f.StringVar(&c.translateArgs.Vertexes, "vertexes", "", "File with vertexes")
	f.StringVar(&c.translateArgs.VertexesOut, "vertexes-out", "", "File with vertexes result")
	f.StringVar(&c.translateArgs.Edges, "edges", "", "File with edges")
	f.StringVar(&c.translateArgs.EdgesOut, "edges-out", "", "File with edges out")
	f.StringVar(&c.translateArgs.EdgesMap, "edge-map", "", "File with edge map")

	f = cmd.PersistentFlags()

	f.IntVar(&c.threads, "threads", 32, "Threads")
	f.IntVar(&c.batch, "batch", 1024*1024*4, "Batch size")
	f.StringVar(&c.logLevel, "log-level", "info", "Define log level")

	cmd.AddCommand(extractVertexes, extractEdges, mapEdges, optimize, translate)

	return cmd
}

func (c *cli) extractVertexesPre(cmd *cobra.Command, args []string) error {
	if err := c.markAsRequired(c.extractVertexesArgs.In != "", "--in parameter is not set"); err != nil {
		return err
	}
	if err := c.markAsRequired(c.extractVertexesArgs.Out != "", "--out parameter is not set"); err != nil {
		return err
	}
	return nil
}

func (c *cli) extractVertexes(cmd *cobra.Command, args []string) error {
	logger := c.getLogger(cmd)

	inF, err := os.OpenFile(c.extractVertexesArgs.In, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}

	defer inF.Close()

	outF, err := os.OpenFile(c.extractVertexesArgs.Out, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	defer outF.Close()

	var failed bool
	h := NewHandler(logger)

	ExtractVertexFromDocuments(h, inF, outF, c.threads)

	go h.Wait()

	for err := range h.Errors() {
		failed = true

		logger.Error().Err(err).Msgf("Failed to parse data")
	}

	if failed {
		return errors.Errorf("Data parsing failed")
	}

	return nil
}

func (c *cli) extractEdgesPre(cmd *cobra.Command, args []string) error {
	if err := c.markAsRequired(c.extractEdgesArgs.In != "", "--in parameter is not set"); err != nil {
		return err
	}
	if err := c.markAsRequired(c.extractEdgesArgs.Out != "", "--out parameter is not set"); err != nil {
		return err
	}
	return nil
}

func (c *cli) extractEdges(cmd *cobra.Command, args []string) error {
	logger := c.getLogger(cmd)

	inF, err := os.OpenFile(c.extractEdgesArgs.In, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}

	defer inF.Close()

	outF, err := os.OpenFile(c.extractEdgesArgs.Out, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	defer outF.Close()

	var failed bool
	h := NewHandler(logger)

	ExtractEdgeVertexFromEdge(h, inF, outF, c.threads)

	go h.Wait()

	for err := range h.Errors() {
		failed = true

		logger.Error().Err(err).Msgf("Failed to parse data")
	}

	if failed {
		return errors.Errorf("Data parsing failed")
	}

	return nil
}

func (c *cli) mapEdgesPre(cmd *cobra.Command, args []string) error {
	if err := c.markAsRequired(c.mapEdgesArgs.Vertexes != "", "--vertex parameter is not set"); err != nil {
		return err
	}
	if err := c.markAsRequired(c.mapEdgesArgs.Edges != "", "--edge parameter is not set"); err != nil {
		return err
	}
	if err := c.markAsRequired(c.mapEdgesArgs.TempA != "", "--temp.a parameter is not set"); err != nil {
		return err
	}
	if err := c.markAsRequired(c.mapEdgesArgs.TempB != "", "--temp.b parameter is not set"); err != nil {
		return err
	}
	if err := c.markAsRequired(c.mapEdgesArgs.Out != "", "--out parameter is not set"); err != nil {
		return err
	}
	return nil
}

func (c *cli) mapEdges(cmd *cobra.Command, args []string) error {
	logger := c.getLogger(cmd)

	inF, err := os.OpenFile(c.mapEdgesArgs.Vertexes, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}

	defer inF.Close()

	var failed bool

	h := NewHandler(logger)

	MapVertexesAndEdges(h, inF, c.mapEdgesArgs.Edges, c.mapEdgesArgs.TempA, c.mapEdgesArgs.TempB, c.mapEdgesArgs.Out, c.batch, c.threads)

	go h.Wait()

	for err := range h.Errors() {
		failed = true

		logger.Error().Err(err).Msgf("Failed to map data")
	}

	//for err := range MapVertexesAndEdges(inF, c.mapEdgesArgs.Edges, c.mapEdgesArgs.TempA, c.mapEdgesArgs.TempB, c.mapEdgesArgs.Out, c.batch, c.threads) {
	//	failed = true
	//
	//	logger.Error().Err(err).Msgf("Failed to map data")
	//}

	if failed {
		return errors.Errorf("Data parsing failed")
	}

	return nil
}

func (c *cli) optimizePre(cmd *cobra.Command, args []string) error {
	if err := c.markAsRequired(c.optimizeArgs.In != "", "--in parameter is not set"); err != nil {
		return err
	}
	if err := c.markAsRequired(c.optimizeArgs.Out != "", "--out parameter is not set"); err != nil {
		return err
	}
	return nil
}

func (c *cli) optimize(cmd *cobra.Command, args []string) error {
	logger := c.getLogger(cmd)

	inF, err := os.OpenFile(c.optimizeArgs.In, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}

	defer inF.Close()

	outF, err := os.OpenFile(c.optimizeArgs.Out, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	defer outF.Close()

	var failed bool

	h := NewHandler(logger)

	RunOptimization(h, inF, outF)

	go h.Wait()

	for err := range h.Errors() {
		failed = true

		logger.Error().Err(err).Msgf("Failed to map data")
	}

	if failed {
		return errors.Errorf("Data parsing failed")
	}

	return nil
}

func (c *cli) translatePre(cmd *cobra.Command, args []string) error {
	if err := c.markAsRequired(c.translateArgs.Map != "", "--map parameter is not set"); err != nil {
		return err
	}
	if err := c.markAsRequired(c.translateArgs.Vertexes != "", "--vertexes parameter is not set"); err != nil {
		return err
	}
	if err := c.markAsRequired(c.translateArgs.VertexesOut != "", "--vertexes-out parameter is not set"); err != nil {
		return err
	}
	if err := c.markAsRequired(c.translateArgs.Edges != "", "--edges parameter is not set"); err != nil {
		return err
	}
	if err := c.markAsRequired(c.translateArgs.EdgesOut != "", "--edges-out parameter is not set"); err != nil {
		return err
	}
	if err := c.markAsRequired(c.translateArgs.EdgesMap != "", "--edge-map parameter is not set"); err != nil {
		return err
	}
	return nil
}

func (c *cli) translate(cmd *cobra.Command, args []string) error {
	logger := c.getLogger(cmd)

	mapInF, err := os.OpenFile(c.translateArgs.Map, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}

	defer mapInF.Close()

	vertexesInF, err := os.OpenFile(c.translateArgs.Vertexes, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}

	defer vertexesInF.Close()

	vertexesOutF, err := os.OpenFile(c.translateArgs.VertexesOut, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	defer vertexesOutF.Close()

	edgesOutF, err := os.OpenFile(c.translateArgs.EdgesOut, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	defer edgesOutF.Close()

	edgesInF, err := os.OpenFile(c.translateArgs.Edges, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}

	defer edgesInF.Close()

	edgeMapIn, err := os.OpenFile(c.translateArgs.EdgesMap, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}

	defer edgeMapIn.Close()

	var failed bool

	h := NewHandler(logger)

	RunTranslation(h, mapInF, vertexesInF, edgesInF, edgeMapIn, vertexesOutF, edgesOutF, c.threads)

	go h.Wait()

	for err := range h.Errors() {
		failed = true

		logger.Error().Err(err).Msgf("Failed to map data")
	}

	if failed {
		return errors.Errorf("Data parsing failed")
	}

	return nil
}

func (c *cli) getLogger(cmd *cobra.Command) zerolog.Logger {
	level, err := zerolog.ParseLevel(c.logLevel)
	if err != nil {
		level = zerolog.InfoLevel
	}

	return zerolog.New(cmd.OutOrStdout()).Level(level).With().Timestamp().Logger()
}

func (c *cli) markAsRequired(set bool, message string, args ...interface{}) error {
	if !set {
		return errors.Errorf(message, args...)
	}

	return nil
}
