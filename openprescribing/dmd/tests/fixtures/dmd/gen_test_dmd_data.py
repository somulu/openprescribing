# This script generates XML files which contain a self-consitent subset of the
# dm+d data.  The VMP, VMPP, AMP, AMPP, and GTIN files are all trimmed so that
# they only contain the objects related to the VMPs that are passed to the
# script.  The new XML files have the same filenames as the originals, and are
# copied to a new (user-provided) location.
#
# The other dm+d data files are copied to the new location unchanged.  It would
# be straightforward to trim the VTM (virtual therapeutic moieties) and the
# ingredients file, and a bit fiddlier to trim the lookups file.
#
# Additionally the .xsd schema definitions are copied to the new location.
#
# Usage:
#
#   python gen_test_dmd_data.py [inp_dir] [outp_dir] [vpids]
#
# Where:
#
#   inp_dir is the path of a directory containing the unzipped dm+d data files
#   outp_dir is the path of a directory where the new files will be written to
#   vpids is a comma-separated list of IDs of VMPs


from __future__ import print_function

from glob import glob
import os
import shutil
import sys

from lxml import etree

if len(sys.argv) != 4:
    print ("Usage: python gen_test_dmd_data.py [inp_dir] [outp_dir] [vpids]")
    sys.exit(1)

inp_dir = sys.argv[1]
outp_dir = sys.argv[2]
vpids = sys.argv[3].split(",")

try:
    os.makedirs(outp_dir)
except OSError as exc:
    import errno

    if exc.errno == errno.EEXIST and os.path.isdir(outp_dir):
        pass
    else:
        raise

ids = {"vmp": vpids, "vmpp": [], "amp": [], "ampp": []}


# Handle VMPs, VMPPs, AMPs, AMPPs
for obj_type in ["vmp", "vmpp", "amp", "ampp"]:
    # The name of the ID field (eg VPID for a VMP, etc)
    id_field_name = obj_type.replace("m", "").upper() + "ID"

    paths = glob(os.path.join(inp_dir, "f_{}2_*.xml".format(obj_type)))
    assert len(paths) == 1
    path = paths[0]
    filename = os.path.basename(path)
    print (filename)

    with open(path) as f:
        tree = etree.parse(f)

    root = tree.getroot()
    collection_elts = list(root)

    # The first element is a comment, which we'll update.
    comment = collection_elts[0]
    assert isinstance(comment, etree._Comment)
    assert comment.text == " Generated by NHSBSA PPD "
    comment.text += "(and modified by gen_test_dmd_data.py) "

    # The second element is a list of dm+d objects (either VMPs, VMPPs, AMPs,
    # or AMPPs).  We'll remove any objects we don't care about.
    objs = collection_elts[1]
    assert objs.tag == obj_type.upper() + "S"

    for obj in objs:
        if obj_type == "ampp":
            # AMPPs have a VPPID field
            found_matching_obj = obj.find("VPPID").text in ids["vmpp"]
        else:
            # Other object types (VMPs, VMPPs, AMPs) have a VPID field
            found_matching_obj = obj.find("VPID").text in ids["vmp"]

        if found_matching_obj:
            # This is an object we care about!
            if obj_type != "vmp":
                # We want to keep track of the objects we've seen, so we can
                # pull out any properties related to these objects we see
                # later.
                id_ = obj.find(id_field_name).text
                ids[obj_type].append(id_)
        else:
            # This is not an object we care about, so we'll remove it from the
            # tree.
            objs.remove(obj)

    # The subsequent elements are lists of properties that belong to the
    # objects defined in the first element.  We only want properties related to
    # the objects we care about, and remove the rest.
    for collection_elt in collection_elts[2:]:
        if collection_elt.tag == "COMB_CONTENT":
            # We'll skip these elements entirely for now because they relate to
            # combination packs, which we don't deal with in any special way at
            # the moment.
            root.remove(collection_elt)
            continue

        for elt in collection_elt:
            id_ = elt.find(id_field_name).text
            if id_ not in ids[obj_type]:
                # This is not a property we care about, so we'll remove it.
                collection_elt.remove(elt)

    out_path = os.path.join(outp_dir, filename)
    tree.write(out_path, pretty_print=True)


# Handle ingredients, lookups, VTMs
for obj_type in ["ingredient", "lookup", "vtm"]:
    paths = glob(os.path.join(inp_dir, "f_{}2_*.xml".format(obj_type)))
    assert len(paths) == 1
    path = paths[0]
    filename = os.path.basename(path)
    print (filename)

    with open(path) as f:
        tree = etree.parse(f)

    out_path = os.path.join(outp_dir, filename)
    tree.write(out_path, pretty_print=True)


# Handle GTINs
paths = glob(os.path.join(inp_dir, "f_gtin2_*"))
assert len(paths) == 1
path = paths[0]
filename = os.path.basename(path)
print (filename)

with open(path) as f:
    tree = etree.parse(f)

root = tree.getroot()
collection_elts = list(root)

# The first element is a comment, which we'll update.
comment = collection_elts[0]
assert isinstance(comment, etree._Comment)
assert "Generated by NHSBSA PPD" in comment.text
comment.text += "(and modified by gen_test_dmd_data.py) "

# The second element is a list of AMPPs.  We'll remove any objects we don't
# care about.
ampps = collection_elts[1]
assert objs.tag == "AMPPS"

for ampp in ampps:
    # AMPPID is not a typo
    found_matching_obj = ampp.find("AMPPID").text in ids["ampp"]

    if not found_matching_obj:
        # This is not an object we care about, so we'll remove it from the
        # tree.
        ampps.remove(ampp)

out_path = os.path.join(outp_dir, filename)
tree.write(out_path, pretty_print=True)


# Handle .xsd files
for path in glob(os.path.join(inp_dir, "*.xsd")):
    shutil.copy(path, outp_dir)
