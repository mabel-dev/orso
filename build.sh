#!/bin/bash
set -ex

cd $GITHUB_WORKSPACE/io
cd io

# Resolve /opt/python interpreter directories robustly for variants like cp314t or cp314T
py_tag="${PYTHON_VERSION//./}"
PYBIN_CANDIDATES=()

# If the tag ends with a t/T (free-threaded), try with and without the suffix and both cases
if [[ "$py_tag" =~ ^([0-9]+)([Tt])$ ]]; then
  base="${BASH_REMATCH[1]}"
  PYBIN_CANDIDATES+=("/opt/python/cp${py_tag}-cp${py_tag}/bin")
  PYBIN_CANDIDATES+=("/opt/python/cp${base}-cp${base}/bin")
  PYBIN_CANDIDATES+=("/opt/python/cp${base}T-cp${base}T/bin")
  PYBIN_CANDIDATES+=("/opt/python/cp${base}t-cp${base}t/bin")
else
  PYBIN_CANDIDATES+=("/opt/python/cp${py_tag}-cp${py_tag}/bin")
fi

# Pick the first candidate that exists
PYBIN=""
for c in "${PYBIN_CANDIDATES[@]}"; do
  if [ -x "${c}/python" ]; then
    PYBIN="$c"
    break
  fi
done

if [ -z "$PYBIN" ]; then
  echo "No matching /opt/python interpreter found for PYTHON_VERSION=${PYTHON_VERSION}"
  echo "Tried these candidates:"
  for c in "${PYBIN_CANDIDATES[@]}"; do echo "  - $c"; done
  echo "Available /opt/python entries:"; ls -1 /opt/python || true
  exit 1
fi

# Install necessary packages
"${PYBIN}/python" -m pip install -U setuptools wheel numpy cython auditwheel

# Build the wheel
"${PYBIN}/python" setup.py bdist_wheel

# Check if this is a free-threaded build
IS_FREE_THREADED=false
if [[ "$PYTHON_VERSION" =~ [0-9]+\.[0-9]+[Tt]$ ]]; then
    IS_FREE_THREADED=true
    echo "Building free-threaded Python wheel (PYTHON_VERSION=$PYTHON_VERSION)"
fi

# Repair the wheel using auditwheel
for whl in dist/*.whl; do
    [ -f "$whl" ] || continue
    echo "Processing wheel: $whl"
    
    if [ "$IS_FREE_THREADED" = true ]; then
        echo "  -> Free-threaded build detected"
        
        auditwheel repair "$whl" -w dist/
        
        # Rename the repaired wheel to restore the 't' suffix in ABI tag
        # auditwheel strips the 't', so: cp314-cp314 -> cp314-cp314t
        repaired=$(ls -t dist/*manylinux*.whl 2>/dev/null | head -n1)
        if [ -f "$repaired" ]; then
            # Restore 't' suffix on second ABI tag: cp314-cp314 -> cp314-cp314t
            restored=$(echo "$repaired" | sed -E 's/-cp([0-9]+)-cp([0-9]+)-manylinux/-cp\1-cp\2t-manylinux/')
            if [ "$repaired" != "$restored" ]; then
                mv -v "$repaired" "$restored"
                echo "  -> Restored free-threaded ABI tag: $(basename $restored)"
            else
                echo "  -> Already has correct tag: $(basename $repaired)"
            fi
        fi
    else
        echo "  -> Standard build"
        auditwheel repair "$whl" -w dist/
    fi
done

# Show final wheels
echo "=== Wheels after repair ==="
ls -lh dist/*manylinux*.whl || echo "No manylinux wheels found"