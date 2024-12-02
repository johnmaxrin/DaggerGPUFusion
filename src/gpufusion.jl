using TaskLocalValues
using Graphs
using GraphPlots
export gpufuse


struct FusionTaskQueue <: AbstractTaskQueue
    running_tasks::Vector{}
    FusionTaskQueue() = new([])
end


function Dagger.enqueue!(queue::FusionTaskQueue, spec::Pair{Dagger.DTaskSpec, DTask})
     push!(queue.running_tasks, (spec))    
end

function gpufuse(f)
    queue = FusionTaskQueue()
    with_options(f; task_queue = queue) 
    dofusion(queue)

    # upper = get_options(:task_queue, EagerTaskQueue())
    # enqueue!(upper, queue.running_tasks)
    # println(queue)

end


function dofusion(queue)
    
    # Condtions to fuse two kernels
        # 1. kernels are small and lightweight
        # 2. kernels share input/output data

    # Donot fuse if (Future)
        # 1. kernels can be independently parallelized and 
        #    system has abundant computational resources.
        # 2. Do not fuse if tasks have different 
        #    computational characteristics
        
    # Check if kernels share input and output data. 

    task_mem_ref = Vector{Tuple{Int,Int}}()
    task_id = 1
    for (spec, task) in collect(queue.running_tasks)
        for i in spec.args
            push!(task_mem_ref,(task_id,Int(i.second.data.rc.obj.mem.ptr)))
        end
        task_id += 1
    end


    mem_to_task = Dict{Int, Vector{Int}}()
    
    for(task_id, mem_ref) in task_mem_ref
        push!(get!(mem_to_task, mem_ref,[]), task_id)
    end

    g = SimpleGraph(task_id)
    for tasks in values(mem_to_task)
        for i in 1:length(tasks)
            for j in i+1:length(tasks)
                add_edge!(g, tasks[i], tasks[j])
            end
        end
    end 



    
end


