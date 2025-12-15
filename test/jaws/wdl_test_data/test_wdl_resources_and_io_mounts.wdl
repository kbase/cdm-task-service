
version 1.0

# CTS_JOB_ID: foo

workflow some_image {
  input {
      Array[Array[File]] input_files_list
      Array[Array[String]] file_locs_list
      Array[Array[String]] environment_list
      Array[Array[String]] cmdline_list
      Array[Int] container_num_list
  }
  scatter (i in range(length(input_files_list))) {
    call run_container {
      input:
        input_files = input_files_list[i],
        file_locs = file_locs_list[i],
        environ = environment_list[i],
        cmdline = cmdline_list[i],
        container_num = container_num_list[i]
    }
  }
  output {
    Array[Array[File]] output_files = run_container.output_files
    Array[File] stdouts = run_container.stdout
    Array[File] stderrs = run_container.stderr
  }
}

task run_container {
  input {
    Array[File] input_files
    Array[String] file_locs
    Array[String] environ
    Array[String] cmdline
    Int container_num
  }
  command <<<
    # ensure host mount points exist
    mkdir -p ./__input__
    mkdir -p ./__output__
    
    
    # link the input files into the mount point
    files=('~{sep="' '" input_files}')
    locs=(~{sep=" " file_locs})
    for i in ${!files[@]}; do
        mkdir -p ./__input__/$(dirname ${locs[i]})
        ln ${files[i]} ./__input__/${locs[i]}
    done
    
    # Set up environment
    job_env=(~{sep=" " environ})
    for jenv in ${job_env[@]}; do
        export $jenv
    done
    
    # run the command
    ~{sep=" " cmdline}
    EC=$?
    echo "Entrypoint exit code: $EC"

    # list the output of the command
    find ./__output__ -type f > ./output_files.txt
    
    exit $EC
  >>>
  
  output {
    Array[File] output_files = read_lines("output_files.txt")
    File stdout = "stdout"
    File stderr = "stderr"
  }
  
  runtime {
    docker: "some_image@digest"
    runtime_minutes: 720
    memory: "1000000000006 B"
    cpu: 256
    dynamic_input: "__input__:/woot/thing"
    dynamic_output: "__output__:/outyout"
  }
}
