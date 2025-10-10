#!/usr/bin/env bash
# Given a list of SRA accessions (e.g., from the sample_ids column of a row in your filtered table filtered_hashes_no_Serratus.parquet)
# NOTE: I intentionally used a --sig-dir with no sketches in it, as I found that `sourmash sig kmers` is horrifically
# slow. See: https://matrix.to/#/!gsCtamVSdOOWwDWBIT:gitter.im/$IQgvkLQ1THJX862CgvaFQqIABdHl7-CJ5VTPOZB8txw?via=gitter.im&via=matrix.org&via=matrix.freyachat.eu
# at: https://app.gitter.im/#/room/#sourmash-bio_community:gitter.im Message in a reply at 10/8/2025 to a thread
# started 10/7/2025
cd /scratch/dmk333_new/unclassified_frequent_Logan_metagenome_hashes
python -m logan_dark_matter_extractor.cli annotate \
  --index-root /scratch/logan_darkmatter/novel_index \
  --workdir /scratch/logan_darkmatter/work \
  --sig-dir  /scratch/logan_darkmatter \
  --overwrite \
  DRR019503 DRR045396 DRR045397 DRR067587 DRR067588 DRR067589 DRR067590 DRR067591 DRR092708
