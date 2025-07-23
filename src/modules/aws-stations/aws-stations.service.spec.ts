import { Test, TestingModule } from '@nestjs/testing';
import { AwsStationsService } from './aws-stations.service';

describe('AwsStationsService', () => {
  let service: AwsStationsService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [AwsStationsService],
    }).compile();

    service = module.get<AwsStationsService>(AwsStationsService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
