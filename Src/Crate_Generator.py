import argparse
import os
from rocrate.rocrate import ROCrate
from rocrate.model.contextentity import ContextEntity

def generate_folder_rocrate(input_folder):
    """
    Generates an RO-Crate metadata file describing all CSV files 
    found recursively within the target folder.
    """
    crate = ROCrate()

    # 1. Define the Context/ Affiliation (Area Science Park -> RIT -> LAGE/LADE) 
    area_science_park = crate.add(
        ContextEntity(crate, "#area-science-park", properties={
            "@type": "Organization",
            "name": "Area Science Park"
        })
    )

    rit = crate.add(
        ContextEntity(crate, "#rit", properties={
            "@type": "Organization",
            "name": "Research and Technology Institute (RIT)",
            "parentOrganization": {"@id": area_science_park.id}
        })
    )

    lade = crate.add(
        ContextEntity(crate, "#lade", properties={
            "@type": "Organization",
            "name": "Laboratory of Data Engineering (LADE)",
            "url": "https://www.areasciencepark.it/infrastrutture-di-ricerca/data-engineering-lade/",
            "parentOrganization": {"@id": rit.id}
        })
    )

    lage = crate.add(
        ContextEntity(crate, "#lage", properties={
            "@type": "Organization",
            "name": "Laboratory of Genomics and Epigenomics (LAGE)",
            "url": "https://www.areasciencepark.it/en/research-infrastructures/life-sciences/lage-genomics-and-epigenomics-laboratory/",
            "parentOrganization": {"@id": rit.id}
        })
    )

    # 2. Define the Script (Provenance)
    ro_crate_script = crate.add(ContextEntity(crate, "#folder-analyzer-script", properties={
        "@type": "SoftwareApplication",
        "name": "LAGE Folder Descriptor Generator",
        "description": "Script to generate RO-Crate descriptors for lab data folders recursively",
        "creator": {"@id": lade.id}
    }))

    # 3. Scan the folder RECURSIVELY for CSV files
    print(f"Recursively scanning folder: {input_folder}")
    
    count = 0
    # os.walk yields a 3-tuple (dirpath, dirnames, filenames)
    # root: The current folder it is looking at. os.walk updates root every time it moves into a new subfolder.
    # dirs: A list of subfolders in the current folder.
    # files: A list of all files in the current folder
    for root, dirs, files in os.walk(input_folder):
        for filename in files:
            if filename.lower().endswith('.csv'):
                full_path = os.path.join(root, filename)
                
                #  Calculate the relative path from the input_folder
                # This ensures the @id in the JSON-LD is "subdir/file.csv" rather than a local absolute path
                rel_path = os.path.relpath(full_path, input_folder)
                
                file_entity = crate.add_file(full_path, dest_path=rel_path, properties={
                    "description": f"Raw instrument data file: {filename}",
                    "creator": {"@id": lage.id},
                    "encodingFormat": "text/csv",
                    'wasGeneratedBy' :  {"@id": ro_crate_script.id}
                })
                print(f" Entity added to Crate: {rel_path}")
                count += 1

    if count == 0:
        print("⚠️ No CSV files found in the directory structure. Crate will be empty.")
    

    # 4. Save the manifest
    # This will place ro-crate-metadata.json inside the input_folder
    crate.write(input_folder)
    print(f"\n✅ Success! Processed {count} files.")
    print(f"Generated 'ro-crate-metadata.json' in: {input_folder}")

 
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate recursive RO-Crate for CSV files.")
    parser.add_argument("folder_path", help="The root folder to scan")
    args = parser.parse_args()
    
    if os.path.isdir(args.folder_path):
        generate_folder_rocrate(args.folder_path)
    else:
        print(f"❌ Error: {args.folder_path} is not a valid directory.")