# ge-orchestrator: its own static binary plus the ge-agent and ge-mcp binaries
# it spawns, composed from their published images at PINNED tags (bump the ARGs
# deliberately — see each repo's VERSION). DIRECTIVE.md rides in from ge-agent.

ARG GE_AGENT_IMAGE=ghcr.io/oldschool-market-research/ge-agent:0.2.1
ARG GE_MCP_IMAGE=ghcr.io/oldschool-market-research/ge-mcp:0.1.0

FROM ${GE_AGENT_IMAGE} AS agent
FROM ${GE_MCP_IMAGE}   AS mcp

# ---- build ----
FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags="-s -w" -o /out/ge-orchestrator .

# ---- runtime ----
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ge-orchestrator /ge-orchestrator
COPY --from=agent /ge-agent /bin/ge-agent
COPY --from=agent /etc/ge-agent/DIRECTIVE.md /etc/ge-agent/DIRECTIVE.md
COPY --from=mcp   /ge-mcp   /bin/ge-mcp
USER nonroot:nonroot
ENV GE_AGENT_PATH=/bin/ge-agent \
    GE_MCP_PATH=/bin/ge-mcp \
    GE_AGENT_DIRECTIVE=/etc/ge-agent/DIRECTIVE.md \
    GE_ORCH_STATE=/state \
    GE_ORCH_ADDR=0.0.0.0:8410
ENTRYPOINT ["/ge-orchestrator"]
