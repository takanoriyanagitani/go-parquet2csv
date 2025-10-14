#!/bin/sh

ipname=./sample.d/input.parquet

sampleinputjsons(){
	jq -c -n '{timestamp:"2025-10-14T00:01:23.456789Z",severity:"INFO",status:200}'
	jq -c -n '{timestamp:"2025-10-13T00:01:23.456789Z",severity:"WARN",status:500}'
}

geninput(){
	echo generating input file...

	mkdir -p ./sample.d

	which jsons2parquet | fgrep -q jsons2parquet || exec sh -c '
		echo jsons2parquet missing.
		echo see github.com/takanoriyanagitani/go-jsons2parquet to install it.
		exit 1
	'

	sampleinputjsons |
		jsons2parquet |
		cat > "${ipname}"
}

test -f "${ipname}" || geninput

echo
echo showing the parquet using parquet-read...
parquet-read "${ipname}"

echo
echo showing the parquet using parquet2csv...
./parquet2csv \
	-header=true \
	"${ipname}"
